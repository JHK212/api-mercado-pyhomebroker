# greeks.py
from math import log, sqrt, exp, erf, pi

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))

def _norm_pdf(x: float) -> float:
    return 1.0 / sqrt(2.0 * pi) * exp(-0.5 * x * x)

def bs_price(S, K, T, r, sigma, option_type: str):
    """Precio Black–Scholes (sin dividendos, por ahora)."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    
    d1 = (log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    
    if option_type.upper() == "C":  # Call
        return S * _norm_cdf(d1) - K * exp(-r * T) * _norm_cdf(d2)
    else:  # Put
        return K * exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)

def implied_vol(target_price, S, K, T, r, option_type: str,
                tol: float = 1e-4, max_iter: int = 100):
    """
    IV por bisección simple. Devuelve sigma o None si no converge.
    """
    if target_price is None or target_price <= 0:
        return None
    
    low, high = 1e-4, 5.0  # [0.01%, 500%]
    
    for _ in range(max_iter):
        mid = 0.5 * (low + high)
        price = bs_price(S, K, T, r, mid, option_type)
        if price is None:
            return None
        
        diff = price - target_price
        
        if abs(diff) < tol:
            return mid
        
        if diff > 0:
            high = mid
        else:
            low = mid
    
    return mid  # última aproximación

def bs_greeks(S, K, T, r, sigma, option_type: str):
    """Devuelve delta, gamma, vega, theta (theta anual)."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return {"delta": None, "gamma": None, "vega": None, "theta": None}
    
    d1 = (log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    
    if option_type.upper() == "C":
        delta = _norm_cdf(d1)
        theta = (-S * _norm_pdf(d1) * sigma / (2 * sqrt(T))
                 - r * K * exp(-r * T) * _norm_cdf(d2))
    else:
        delta = _norm_cdf(d1) - 1
        theta = (-S * _norm_pdf(d1) * sigma / (2 * sqrt(T))
                 + r * K * exp(-r * T) * _norm_cdf(-d2))
    
    gamma = _norm_pdf(d1) / (S * sigma * sqrt(T))
    vega = S * _norm_pdf(d1) * sqrt(T)  # por 1 punto de vol (1 = 100%)
    
    return {
        "delta": delta,
        "gamma": gamma,
        "vega": vega,
        "theta": theta
    }
