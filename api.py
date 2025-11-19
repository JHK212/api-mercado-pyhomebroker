import os
from fastapi import FastAPI, HTTPException, Query, Depends, Header, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel

from hb_service import hb_service, dataframe_to_records

import pandas as pd


load_dotenv()


class BatchRequest(BaseModel):
    symbols: List[str]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    hb_service.start()
    yield
    # Shutdown
    hb_service.stop()


app = FastAPI(title="HB API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de seguridad - API PÚBLICA
# Logging para debug
import logging
logger = logging.getLogger(__name__)
logger.info("API configurada en modo PÚBLICO - sin autenticación requerida")


@app.get("/")
def root():
    return {
        "message": "HB API funcionando", 
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "options": "/options",
            "options_by_prefix": "/options/prefix/{prefix}",
            "options_by_ticker": "/options/ticker/{ticker}",
            "options_all": "/options/all",
            "stocks": "/stocks",
            "stocks_by_prefix": "/stocks/prefix/{prefix}",
            "stocks_by_ticker": "/stocks/ticker/{ticker}",
            "stocks_all": "/stocks/all",
            "securities": "/securities",
            "securities_by_ticker": "/securities/ticker/{ticker}",
            "securities_all": "/securities/all",
            "cauciones": "/cauciones",
            "config": "/config",
            "docs": "/docs"
        },
        "authentication": "API PÚBLICA - No se requiere autenticación"
    }


@app.get("/health")
def health():
    """Endpoint mejorado de health check con información detallada"""
    connection_status = hb_service.get_connection_status()
    
    # Determinar estado general
    if connection_status["connected"] and connection_status["receiving_data"]:
        status = "healthy"
    elif connection_status["connected"]:
        status = "connected_but_stale"
    else:
        status = "disconnected"
    
    return {
        "status": status,
        "connected": connection_status["connected"],
        "receiving_data": connection_status["receiving_data"],
        "last_data_received": connection_status["last_data_received"],
        "minutes_since_last_data": connection_status["minutes_since_last_data"],
        "connection_attempts": connection_status["connection_attempts"]
    }


@app.get("/options")

def get_options(
    prefix: Optional[str] = Query(None, description="Filtrar opciones por prefijo"),
    ticker: Optional[str] = Query(None, description="Filtrar opciones por ticker específico"),
    with_greeks: bool = Query(False, description="Si es true, agrega IV y griegas")
):
    """
    Obtiene todas las opciones o filtra por prefijo/ticker.
    
    - Sin parámetros: retorna todas las opciones (aplicando filtros por defecto)
    - prefix: filtra opciones que empiecen con el prefijo (ej: GFG)
    - ticker: filtra opciones por ticker específico
    - with_greeks=true: agrega columnas de IV y griegas
    """
    try:
        df = hb_service.get_options(prefix=prefix, ticker=ticker)

        if with_greeks:
            # Calcula IV y griegas
            df = hb_service._attach_greeks(df)

            # (opcional) aseguramos columna underlying_asset:
            # si ya la estás seteando en _attach_greeks, esto no hace falta.
            if "underlying_asset" not in df.columns:
                # ejemplo simple: tomar prefijo y mapearlo a subyacente
                # acá podés usar el mismo mapping que en _get_underlying_price
                df["underlying_asset"] = None
                for idx in df.index:
                    sym = str(idx).upper()
                    pref = sym[:4]
                    mapping = {
                        "GFGC": "GGAL",
                        "GFGV": "GGAL",
                        "YPFC": "YPFD",
                    }
                    df.at[idx, "underlying_asset"] = mapping.get(pref)

        return dataframe_to_records(df)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo opciones: {str(e)}")



@app.get("/options/prefix/{prefix}")
def get_options_by_prefix(prefix: str):
    """
    Obtiene opciones filtradas por prefijo específico.
    
    Ejemplo: /options/prefix/GFG retorna todas las opciones que empiecen con "GFG"
    """
    try:
        df = hb_service.get_options(prefix=prefix)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo opciones por prefijo: {str(e)}")


@app.get("/options/ticker/{ticker}")
def get_options_by_ticker(ticker: str):
    """
    Obtiene opciones por ticker específico.
    
    Ejemplo: /options/ticker/GFG24JAN17.50C retorna solo esa opción
    """
    try:
        df = hb_service.get_options(ticker=ticker)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo opción por ticker: {str(e)}")


@app.get("/options/all")
def get_all_options():
    """
    Obtiene TODAS las opciones disponibles sin filtros.
    
    Este endpoint retorna todas las opciones que están siendo monitoreadas,
    independientemente de los prefijos configurados en tickers.json.
    """
    try:
        # Pasar None para obtener todas las opciones sin filtros
        df = hb_service.get_options(prefix=None, ticker=None)
        return {
            "message": f"Todas las opciones disponibles ({len(df)} instrumentos)",
            "total_count": len(df),
            "data": dataframe_to_records(df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo todas las opciones: {str(e)}")


@app.get("/options/test_greeks")
def test_greeks():
    """
    Endpoint de prueba: usa una opción fake en un DataFrame
    y le aplica _attach_greeks para verificar que IV + griegas funcionan.
    """
    try:
        # Datos mínimos simulados para una opción de ejemplo
        data = {
            "strike": [9000],
            "last": [120.5],
            "bid": [120],
            "ask": [121],
            "expiration": [pd.Timestamp("2025-02-28")],
            "kind": ["CALL"],
        }
        df = pd.DataFrame(data)
        df.index = ["GFGC9000DI"]  # símbolo de opción simulado

        # Usamos la función interna del servicio para adjuntar griegas
        df_with = hb_service._attach_greeks(df)

        return dataframe_to_records(df_with)

    except Exception as e:
        # Esto te permite ver el error real en el JSON
        raise HTTPException(status_code=500, detail=f"Error en test_greeks: {str(e)}")


@app.get("/stocks")
def get_stocks(
    prefix: Optional[str] = Query(None, description="Filtrar acciones por prefijo"),
    ticker: Optional[str] = Query(None, description="Filtrar acciones por ticker específico")
):
    """
    Obtiene todas las acciones o filtra por prefijo/ticker.
    
    - Sin parámetros: retorna todas las acciones (aplicando filtros por defecto)
    - prefix: filtra acciones que empiecen con el prefijo (ej: GGAL)
    - ticker: filtra acciones por ticker específico
    """
    try:
        df = hb_service.get_stocks(prefix=prefix, ticker=ticker)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo acciones: {str(e)}")


@app.get("/stocks/prefix/{prefix}")
def get_stocks_by_prefix(prefix: str):
    """
    Obtiene acciones filtradas por prefijo específico.
    
    Ejemplo: /stocks/prefix/GGAL retorna todas las acciones que empiecen con "GGAL"
    """
    try:
        df = hb_service.get_stocks(prefix=prefix)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo acciones por prefijo: {str(e)}")


@app.get("/stocks/ticker/{ticker}")
def get_stocks_by_ticker(ticker: str):
    """
    Obtiene acciones por ticker específico.
    
    Ejemplo: /stocks/ticker/GGAL retorna solo esa acción
    """
    try:
        df = hb_service.get_stocks(ticker=ticker)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo acción por ticker: {str(e)}")


@app.get("/stocks/all")
def get_all_stocks():
    """
    Obtiene TODAS las acciones disponibles sin filtros.
    
    Este endpoint retorna todas las acciones que están siendo monitoreadas,
    independientemente de los prefijos configurados en tickers.json.
    """
    try:
        # Pasar None para obtener todas las acciones sin filtros
        df = hb_service.get_stocks(prefix=None, ticker=None)
        return {
            "message": f"Todas las acciones disponibles ({len(df)} instrumentos)",
            "total_count": len(df),
            "data": dataframe_to_records(df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo todas las acciones: {str(e)}")


@app.get("/securities")
def get_securities(
    ticker: Optional[str] = Query(None, description="Filtrar securities por ticker específico"),
    type: Optional[str] = Query(None, description="Tipo de security: acciones, bonos, cedears, letras, ons, panel_general")
):
    """
    Obtiene todos los securities o filtra por ticker/tipo.
    
    - Sin parámetros: retorna todos los securities
    - ticker: filtra por ticker específico
    - type: filtra por tipo de instrumento
    """
    try:
        df = hb_service.get_securities(ticker=ticker, type=type)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo securities: {str(e)}")


@app.get("/securities/ticker/{ticker}")
def get_securities_by_ticker(ticker: str):
    """
    Obtiene securities por ticker específico.
    
    Ejemplo: /securities/ticker/GGAL retorna todos los securities de GGAL
    """
    try:
        df = hb_service.get_securities(ticker=ticker)
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo securities por ticker: {str(e)}")


@app.get("/securities/all")
def get_all_securities():
    """
    Obtiene TODOS los securities disponibles sin filtros.
    
    Este endpoint retorna todos los securities que están siendo monitoreados,
    independientemente de los tipos configurados en tickers.json.
    """
    try:
        # Pasar None para obtener todos los securities sin filtros
        df = hb_service.get_securities(ticker=None, type=None)
        return {
            "message": f"Todos los securities disponibles ({len(df)} instrumentos)",
            "total_count": len(df),
            "data": dataframe_to_records(df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo todos los securities: {str(e)}")


@app.get("/cauciones")
def get_cauciones():
    """
    Obtiene datos de cauciones.
    """
    try:
        df = hb_service.get_cauciones()
        return dataframe_to_records(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo cauciones: {str(e)}")


@app.get("/cauciones/1d")
def get_caucion_1d():
    try:
        rate = hb_service._get_caucion_1d_rate()
        if rate is None:
            raise HTTPException(status_code=404, detail="No se pudo determinar tasa de caución 1D")
        
        return {"rate": rate}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo caución 1D: {str(e)}")



@app.get("/status/connection")
def get_connection_status():
    """
    Obtiene el estado de conexión a HomeBroker.
    """
    try:
        status = hb_service.get_connection_status()
        return {
            "message": "Estado de conexión",
            "data": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo estado de conexión: {str(e)}")


@app.get("/config")
def get_config():
    """
    Obtiene la configuración actual de la API con información detallada de conexión.
    """
    try:
        connection_status = hb_service.get_connection_status()
        return {
            "broker_id": hb_service.broker_id,
            "option_prefixes": hb_service.option_prefixes,
            "stock_prefixes": hb_service.stock_prefixes,
            "options_count": len(hb_service.options),
            "stocks_count": len(hb_service.get_stocks()),
            "securities_count": len(hb_service.everything),
            "cauciones_count": len(hb_service.cauciones),
            "tickers_file": os.getenv("HB_TICKERS_FILE", "tickers.json"),
            "connection_status": connection_status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo configuración: {str(e)}")


@app.post("/reconnect")
def force_reconnect():
    """
    Fuerza una reconexión manual a HomeBroker.
    Útil cuando se detecta que los datos están desactualizados.
    """
    try:
        # Marcar como desconectado para que el health monitor reconecte
        with hb_service._lock:
            hb_service._connected = False
            hb_service._connection_attempts = 0  # Resetear contador
            
        return {
            "message": "Reconexión forzada iniciada. El sistema intentará reconectar automáticamente.",
            "status": "reconnecting"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error forzando reconexión: {str(e)}")


