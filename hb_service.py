import os
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

import pandas as pd
from pyhomebroker import HomeBroker
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from greeks import implied_vol, bs_greeks  # <- tus funciones de IV y griegas

# Configurar logging estructurado (una sola vez)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Zona horaria global
TZ = ZoneInfo("America/Argentina/Buenos_Aires")


def _read_json_if_exists(path: str) -> Optional[Dict[str, Any]]:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None
    return None


def _parse_prefixes_env(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(",") if p.strip()]


def _load_option_prefixes_from_config_file() -> List[str]:
    # Usa el archivo tickers.json
    cfg = _read_json_if_exists("tickers.json") or {}
    prefixes = cfg.get("options_prefixes") or []
    if isinstance(prefixes, list):
        return [str(p) for p in prefixes if str(p).strip()]
    return []


def _load_option_prefixes_env_then_file() -> List[str]:
    # 1) Env: HB_OPTIONS_PREFIXES=GFG,GAL,ALGO
    prefixes = _parse_prefixes_env(os.getenv("HB_OPTIONS_PREFIXES"))
    if prefixes:
        return prefixes
    # 2) Archivo tickers.json -> options_prefixes
    return _load_option_prefixes_from_config_file()


def _load_stock_prefixes_from_config_file() -> List[str]:
    # Usa el archivo tickers.json
    cfg = _read_json_if_exists("tickers.json") or {}
    prefixes = cfg.get("stock_prefixes") or []
    if isinstance(prefixes, list):
        return [str(p) for p in prefixes if str(p).strip()]
    return []


def _load_stock_prefixes_env_then_file() -> List[str]:
    # 1) Env: HB_STOCK_PREFIXES=GGAL,PAMP,YPF
    prefixes = _parse_prefixes_env(os.getenv("HB_STOCK_PREFIXES"))
    if prefixes:
        return prefixes
    # 2) Archivo tickers.json -> stock_prefixes
    return _load_stock_prefixes_from_config_file()


class HBService:
    """Servicio que mantiene conexión a HomeBroker y expone snapshots en memoria.

    - Inicializa los DataFrames vacíos que se llenarán con datos en tiempo real
    - Actualiza `options`, `securities` (everything) y `cauciones` vía callbacks
    - Ofrece métodos thread-safe para leer los datos
    """

    def __init__(self) -> None:
        load_dotenv()  # Carga variables desde .env si existe

        self.broker_id = int(os.getenv("HB_BROKER", "0"))
        self.dni = os.getenv("HB_DNI", "")
        self.user = os.getenv("HB_USER", "")
        self.password = os.getenv("HB_PASSWORD", "")
        self.option_prefixes: List[str] = _load_option_prefixes_env_then_file()
        self.stock_prefixes: List[str] = _load_stock_prefixes_env_then_file()

        # Configuración de reconexión
        self.reconnect_interval = int(os.getenv("HB_RECONNECT_INTERVAL", "30"))  # segundos
        self.max_reconnect_attempts = int(os.getenv("HB_MAX_RECONNECT_ATTEMPTS", "5"))
        self.health_check_interval = int(os.getenv("HB_HEALTH_CHECK_INTERVAL", "60"))  # segundos

        # DataFrames vacíos que se llenarán con datos en tiempo real
        self.options = pd.DataFrame()
        self.everything = pd.DataFrame()
        self.cauciones = pd.DataFrame()

        # Sincronización y estado de conexión
        self._lock = threading.RLock()
        self._hb: Optional[HomeBroker] = None
        self._thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None
        self._connected = False
        self._last_data_received = datetime.now()
        self._connection_attempts = 0
        self._should_stop = False

    # -----------------------
    # Callbacks de HomeBroker
    # -----------------------
    def _on_options(self, online, quotes):
        with self._lock:
            try:
                # Actualizar timestamp de última recepción de datos
                self._last_data_received = datetime.now()

                if quotes.empty:
                    return

                this_data = quotes.copy()
                # NO dropeamos expiration/strike/kind porque las necesitamos para IV/griegas
                # this_data = this_data.drop(["expiration", "strike", "kind"], axis=1, errors='ignore')
                this_data["change"] = this_data["change"] / 100
                this_data["datetime"] = pd.to_datetime(this_data["datetime"])
                this_data = this_data.rename(columns={"bid_size": "bidsize", "ask_size": "asksize"})

                # Filtrado por prefijos si están configurados
                if self.option_prefixes and not this_data.empty:
                    idx = this_data.index.astype(str)
                    mask = pd.Series(False, index=this_data.index)
                    for prefix in self.option_prefixes:
                        if prefix:
                            mask = mask | idx.str.startswith(prefix)
                    this_data = this_data[mask]

                if not this_data.empty:
                    # Agregar símbolos nuevos que cumplan el filtro
                    new_index = this_data.index.difference(self.options.index)
                    if len(new_index) > 0:
                        new_data = this_data.loc[new_index]
                        if not new_data.empty:
                            self.options = pd.concat([self.options, new_data], axis=0)
                    # Actualizar existentes
                    self.options.update(this_data)
                    logger.debug(f"Actualizadas {len(this_data)} opciones")

            except Exception as e:
                logger.error(f"Error en _on_options: {e}")
                # Continuar sin fallar

    def _on_securities(self, online, quotes):
        with self._lock:
            try:
                # Actualizar timestamp de última recepción de datos
                self._last_data_received = datetime.now()
                
                if quotes.empty:
                    return
                
                this_data = quotes.copy()
                this_data = this_data.reset_index()
                this_data["symbol"] = this_data["symbol"] + " - " + this_data["settlement"]
                this_data = this_data.drop(["settlement"], axis=1)
                this_data = this_data.set_index("symbol")
                this_data["change"] = this_data["change"] / 100   # 👈 acá estaba el bug
                this_data["datetime"] = pd.to_datetime(this_data["datetime"])
                
                # Primera carga o actualización incremental
                if self.everything.empty:
                    # Primera vez: asignamos todo
                    self.everything = this_data
                else:
                    # Agregar símbolos nuevos
                    new_index = this_data.index.difference(self.everything.index)
                    if len(new_index) > 0:
                        new_data = this_data.loc[new_index]
                        if not new_data.empty:
                            self.everything = pd.concat([self.everything, new_data], axis=0)
                    
                    # Actualizar símbolos ya existentes
                    self.everything.update(this_data)

                logger.debug(f"Actualizados {len(this_data)} securities")
            
            except Exception as e:
                logger.error(f"Error en _on_securities: {e}")
                # Continuar sin fallar





    
    def _on_repos(self, online, quotes):
        with self._lock:
            try:
                # Actualizar timestamp de última recepción de datos
                self._last_data_received = datetime.now()
                
                if quotes.empty:
                    return
                
                this_data = quotes.copy()
                this_data = this_data.reset_index()
                this_data = this_data.set_index("symbol")
                this_data = this_data[["PESOS" in s for s in quotes.index]]
                this_data = this_data.reset_index()
                this_data["settlement"] = pd.to_datetime(this_data["settlement"])
                this_data = this_data.set_index("settlement")
                this_data["last"] = this_data["last"] / 100
                this_data["bid_rate"] = this_data["bid_rate"] / 100
                this_data["ask_rate"] = this_data["ask_rate"] / 100
                this_data = this_data.drop(
                    ["open", "high", "low", "volume", "operations", "datetime"], axis=1
                )
                this_data = this_data[
                    ["last", "turnover", "bid_amount", "bid_rate", "ask_rate", "ask_amount"]
                ]
                
                # 👉 Igual idea: agregar filas nuevas y luego actualizar
                if self.cauciones.empty:
                    self.cauciones = this_data
                else:
                    new_index = this_data.index.difference(self.cauciones.index)
                    if len(new_index) > 0:
                        new_data = this_data.loc[new_index]
                        if not new_data.empty:
                            self.cauciones = pd.concat([self.cauciones, new_data], axis=0)
                    
                    self.cauciones.update(this_data)

                logger.debug(f"Actualizadas {len(this_data)} cauciones")
            
            except Exception as e:
                logger.error(f"Error en _on_repos: {e}")
                # Continuar sin fallar

    def _on_error(self, online, error):
        """Manejo de errores de HomeBroker con reconexión automática"""
        logger.error(f"HomeBroker error: {error}")

        # Marcar como desconectado para que el health monitor lo detecte
        with self._lock:
            self._connected = False

        # El health monitor se encargará de la reconexión

    # -----------------------
    # Conexión
    # -----------------------
    def _connect_and_subscribe(self) -> bool:
        """Conecta a HomeBroker y se suscribe a los feeds. Retorna True si tuvo éxito."""
        try:
            logger.info("Iniciando conexión a HomeBroker...")

            # Desconectar conexión previa si existe
            if self._hb:
                try:
                    self._hb.online.disconnect()
                except Exception:
                    pass

            hb = HomeBroker(
                int(self.broker_id),
                on_options=self._on_options,
                on_securities=self._on_securities,
                on_repos=self._on_repos,
                on_error=self._on_error,
            )

            # Autenticación
            logger.info(f"Autenticando con DNI: {self.dni}")
            hb.auth.login(dni=self.dni, user=self.user, password=self.password, raise_exception=True)

            # Conexión online
            logger.info("Conectando online...")
            hb.online.connect()

            # Suscripciones
            logger.info("Suscribiendo a feeds de datos...")
            hb.online.subscribe_options()
            hb.online.subscribe_securities("bluechips", "24hs")
            hb.online.subscribe_securities("bluechips", "SPOT")
            hb.online.subscribe_securities("government_bonds", "24hs")
            hb.online.subscribe_securities("government_bonds", "SPOT")
            hb.online.subscribe_securities("cedears", "24hs")
            hb.online.subscribe_securities("general_board", "24hs")
            hb.online.subscribe_securities("short_term_government_bonds", "24hs")
            hb.online.subscribe_securities("corporate_bonds", "24hs")
            hb.online.subscribe_repos()

            with self._lock:
                self._hb = hb
                self._connected = True
                self._last_data_received = datetime.now()
                self._connection_attempts = 0

            logger.info("✅ Conexión a HomeBroker exitosa")
            return True

        except Exception as e:
            logger.error(f"❌ Error conectando a HomeBroker: {e}")
            with self._lock:
                self._connected = False
                self._connection_attempts += 1
            return False

    def _health_monitor(self) -> None:
        """Monitor de salud que verifica la conexión y reconecta automáticamente"""
        logger.info("Iniciando monitor de salud...")

        while not self._should_stop:
            try:
                time_since_last_data = datetime.now() - self._last_data_received

                # Si no hemos recibido datos en los últimos 5 minutos, reconectar
                if time_since_last_data > timedelta(minutes=5):
                    logger.warning(
                        f"Sin datos por {time_since_last_data.seconds} segundos. Intentando reconexión..."
                    )
                    self._attempt_reconnection()

                # Si no estamos conectados, intentar reconectar
                elif not self._connected:
                    logger.warning("Conexión perdida. Intentando reconexión...")
                    self._attempt_reconnection()

                time.sleep(self.health_check_interval)

            except Exception as e:
                logger.error(f"Error en health monitor: {e}")
                time.sleep(self.health_check_interval)

    def _attempt_reconnection(self) -> None:
        """Intenta reconectar hasta el límite máximo de intentos"""
        if self._connection_attempts >= self.max_reconnect_attempts:
            logger.error(
                f"Máximo de intentos de reconexión alcanzado ({self.max_reconnect_attempts})"
            )
            time.sleep(self.reconnect_interval * 2)
            self._connection_attempts = 0
            return

        logger.info(
            f"Intento de reconexión {self._connection_attempts + 1}/{self.max_reconnect_attempts}"
        )

        if self._connect_and_subscribe():
            logger.info("🔄 Reconexión exitosa")
        else:
            logger.warning(
                f"Reconexión fallida. Reintentando en {self.reconnect_interval} segundos..."
            )
            time.sleep(self.reconnect_interval)

    def start(self) -> None:
        """Inicia el servicio de HomeBroker con monitoreo automático"""
        logger.info("Iniciando servicio de HomeBroker...")

        self._should_stop = False

        if not (self._thread and self._thread.is_alive()):
            self._thread = threading.Thread(
                target=self._connect_and_subscribe,
                name="hb-thread",
                daemon=False,
            )
            self._thread.start()

        if not (self._health_thread and self._health_thread.is_alive()):
            self._health_thread = threading.Thread(
                target=self._health_monitor,
                name="hb-health",
                daemon=False,
            )
            self._health_thread.start()

    def stop(self) -> None:
        """Detiene el servicio de HomeBroker completamente"""
        logger.info("Deteniendo servicio de HomeBroker...")

        self._should_stop = True

        with self._lock:
            if self._hb:
                try:
                    self._hb.online.disconnect()
                    logger.info("Desconectado de HomeBroker")
                except Exception as e:
                    logger.error(f"Error desconectando: {e}")
            self._connected = False

    # -----------------------
    # Lectura base
    # -----------------------
    def get_options(self, prefix: Optional[str] = None, ticker: Optional[str] = None) -> pd.DataFrame:
        """
        Obtiene opciones con filtros opcionales.
        """
        with self._lock:
            df = self.options.copy()

            if ticker:
                df = df[df.index.astype(str) == ticker.upper()]
            elif prefix:
                df = df[df.index.astype(str).str.startswith(prefix.upper())]
            elif self.option_prefixes:
                mask = pd.Series(False, index=df.index)
                for pref in self.option_prefixes:
                    if pref:
                        mask = mask | df.index.astype(str).str.startswith(pref.upper())
                df = df[mask]

            return df


    def get_options_with_greeks(
        self,
        prefix: Optional[str] = None,
        ticker: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Igual que get_options, pero le agrega:
        underlying_price, iv, delta, gamma, vega, theta.
        """
        # Reutilizamos el filtro normal
        base_df = self.get_options(prefix=prefix, ticker=ticker)

        if base_df.empty:
            return base_df

        # Adjuntamos subyacente, IV y griegas
        enriched = self._attach_greeks(base_df)
        return enriched

    def get_securities(self, ticker: Optional[str] = None, type: Optional[str] = None) -> pd.DataFrame:
        """
        Obtiene securities con filtros opcionales.
        """
        with self._lock:
            df = self.everything.copy()

            if ticker:
                df = df[df.index.astype(str).str.contains(ticker.upper(), na=False)]
            elif type:
                type_mapping = {
                    "acciones": [" - 24hs", " - SPOT"],
                    "bonos": [" - 24hs", " - SPOT"],
                    "cedears": [" - 24hs", " - SPOT"],
                    "letras": [" - 24hs", " - SPOT"],
                    "ons": [" - 24hs", " - SPOT"],
                    "panel_general": [" - 24hs", " - SPOT"],
                }
                if type in type_mapping:
                    suffixes = type_mapping[type]
                    mask = df.index.astype(str).str.contains("|".join(suffixes), na=False)
                    df = df[mask]

            return df

    def get_stocks(self, prefix: Optional[str] = None, ticker: Optional[str] = None) -> pd.DataFrame:
        """
        Obtiene acciones (stocks) con filtros opcionales, similar a get_options.
        """
        with self._lock:
            df = self.everything.copy()

            action_suffixes = [" - 24hs", " - SPOT"]
            mask = df.index.astype(str).str.contains("|".join(action_suffixes), na=False)
            df = df[mask]

            if ticker:
                df = df[
                    df.index.astype(str)
                    .str.replace(" - 24hs", "")
                    .str.replace(" - SPOT", "")
                    == ticker.upper()
                ]
            elif prefix:
                df = df[
                    df.index.astype(str)
                    .str.replace(" - 24hs", "")
                    .str.replace(" - SPOT", "")
                    .str.startswith(prefix.upper())
                ]
            elif self.stock_prefixes:
                mask = pd.Series(False, index=df.index)
                clean_index = (
                    df.index.astype(str)
                    .str.replace(" - 24hs", "")
                    .str.replace(" - SPOT", "")
                )
                for pref in self.stock_prefixes:
                    if pref:
                        mask = mask | clean_index.str.startswith(pref.upper())
                df = df[mask]

            return df

    def get_cauciones(self) -> pd.DataFrame:
        with self._lock:
            return self.cauciones.copy()

    def is_connected(self) -> bool:
        """Verifica si la conexión está activa y recibiendo datos"""
        with self._lock:
            if not self._connected:
                return False

            time_since_last_data = datetime.now() - self._last_data_received
            return time_since_last_data < timedelta(minutes=10)

    def get_connection_status(self) -> Dict[str, Any]:
        """Obtiene información detallada del estado de conexión"""
        with self._lock:
            time_since_last_data = datetime.now() - self._last_data_received

            return {
                "connected": self._connected,
                "receiving_data": time_since_last_data < timedelta(minutes=5),
                "last_data_received": self._last_data_received.isoformat(),
                "minutes_since_last_data": int(time_since_last_data.total_seconds() / 60),
                "connection_attempts": self._connection_attempts,
                "max_reconnect_attempts": self.max_reconnect_attempts,
                "reconnect_interval": self.reconnect_interval,
                "health_check_interval": self.health_check_interval,
            }

    # -----------------------
    # Cálculo tasas, subyacente e IV/griegas
    # -----------------------
    def _get_caucion_1d_rate(self) -> Optional[float]:
        """
        Devuelve una tasa corta (~1D) a partir de self.cauciones.

        Lógica:
        - Toma self.cauciones, que tiene índice = settlement (fecha de vencimiento).
        - Filtra filas que tengan una tasa válida.
        - Ordena por settlement ascendente.
        - Devuelve la tasa de la fila más cercana (menor plazo disponible).
    
        Eso hace que:
        - Un día normal → use la caución 1D.
        - Un viernes (cuando la mínima es 3D) → use esa 3D.
        """
        if self.cauciones.empty:
            return None

        df = self.cauciones.copy()

        # Aseguramos que el índice sea datetime (por las dudas)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce")
            df = df[~df.index.isna()]

        if df.empty:
            return None

    # Nos quedamos solo con filas que tengan alguna tasa
        cols_tasa = []
        for c in ["last", "bid_rate", "ask_rate"]:
            if c in df.columns:
                cols_tasa.append(c)
        if not cols_tasa:
            return None

    # Al menos una de las columnas de tasa no nula
        df = df[df[cols_tasa].notna().any(axis=1)]
        if df.empty:
            return None

    # Ordenamos por settlement (índice)
        df = df.sort_index()

        row = df.iloc[0]  # la de menor plazo

        rate = row.get("last")
        if rate is None or pd.isna(rate):
            rate = row.get("bid_rate") or row.get("ask_rate")

        if rate is None or pd.isna(rate):
            return None

    # Ya viene en decimales (ej 0.20 = 20% anual)
        return float(rate)


    def _get_underlying_price(self, option_symbol: str) -> Optional[float]:
        """
        Estima el ticker del subyacente a partir del símbolo de la opción y devuelve su last.
        """
        if not option_symbol:
            return None

        sym = str(option_symbol).upper()
        pref = sym[:4]

        mapping = {
            "GFGC": "GGAL",
            "GFGV": "GGAL",
            "YPFC": "YPFD",
            # agregar más mappings según tus series
        }

        underlying_ticker = mapping.get(pref)
        if not underlying_ticker:
            return None

        stocks_df = self.get_stocks(ticker=underlying_ticker)
        if stocks_df.empty:
            return None

        row = stocks_df.iloc[0]
        last = row.get("last")
        return float(last) if last is not None else None

    def _attach_greeks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Devuelve un DataFrame con columnas extra:
        underlying_price, iv, delta, gamma, vega, theta.
        """
        if df.empty:
            return df

        df = df.copy()
        now = datetime.now(TZ)
        r = self._get_caucion_1d_rate() or 0.0

        df["underlying_price"] = None
        df["iv"] = None
        df["delta"] = None
        df["gamma"] = None
        df["vega"] = None
        df["theta"] = None

        if "expiration" in df.columns:
            df["expiration"] = pd.to_datetime(df["expiration"], errors="coerce")

        for idx, row in df.iterrows():
            try:
                symbol = str(idx)
                last = row.get("last")
                bid = row.get("bid")
                ask = row.get("ask")
                strike = row.get("strike")
                expiration = row.get("expiration")
                kind = row.get("kind")

                S = self._get_underlying_price(symbol)
                if S is None or S <= 0:
                    continue

                if strike is None or strike <= 0:
                    continue
                K = float(strike)

                if expiration is None or pd.isna(expiration):
                    continue
                exp_dt = expiration.to_pydatetime().replace(tzinfo=TZ)
                days = (exp_dt - now).total_seconds() / 86400.0
                if days <= 0:
                    continue
                T = days / 365.0

                if isinstance(kind, str):
                    k = kind.upper()
                    opt_type = "C" if k.startswith("C") else "P"
                else:
                    opt_type = "C" if symbol.endswith("C") else "P"

                price_for_iv = None
                if bid is not None and ask is not None and bid > 0 and ask > 0:
                    price_for_iv = 0.5 * (float(bid) + float(ask))
                elif last is not None and last > 0:
                    price_for_iv = float(last)

                if price_for_iv is None:
                    continue

                sigma = implied_vol(price_for_iv, S, K, T, r, opt_type)
                if sigma is None:
                    continue

                greeks = bs_greeks(S, K, T, r, sigma, opt_type)

                df.at[idx, "underlying_price"] = S
                df.at[idx, "iv"] = sigma
                df.at[idx, "delta"] = greeks["delta"]
                df.at[idx, "gamma"] = greeks["gamma"]
                df.at[idx, "vega"] = greeks["vega"]
                df.at[idx, "theta"] = greeks["theta"]

            except Exception as e:
                logger.error(f"Error calculando griegas para {idx}: {e}")
                continue

        return df


# Instancia singleton para usar desde la API
hb_service = HBService()


def dataframe_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convierte DataFrame a lista de dicts con fechas serializadas a ISO-8601."""
    try:
        safe_df = df.reset_index()

        for col in safe_df.columns:
            if safe_df[col].dtype in ["float64", "float32"]:
                safe_df[col] = safe_df[col].replace([float("inf"), float("-inf")], None)
                safe_df[col] = safe_df[col].where(pd.notna(safe_df[col]), None)
                safe_df[col] = safe_df[col].apply(
                    lambda x: None
                    if isinstance(x, (int, float))
                    and (abs(x) > 1e15 or (x == 0 and pd.isna(x)))
                    else x
                )
            elif safe_df[col].dtype in ["int64", "int32"]:
                safe_df[col] = safe_df[col].apply(
                    lambda x: None if isinstance(x, (int, float)) and abs(x) > 1e15 else x
                )

        safe_df = safe_df.where(pd.notna(safe_df), None)

        result = safe_df.to_dict(orient="records")

        for row in result:
            for key, value in list(row.items()):
                if isinstance(value, pd.Timestamp):
                    row[key] = value.isoformat()
                elif isinstance(value, (int, float)):
                    if (
                        pd.isna(value)
                        or value == float("inf")
                        or value == float("-inf")
                        or abs(value) > 1e15
                    ):
                        row[key] = None
                elif value in ("nan", "inf", "-inf"):
                    row[key] = None

        return result

    except Exception as e:
        print(f"Error serializando DataFrame: {e}")
        return []
