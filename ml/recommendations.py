from typing import Dict, Any

def recommend_action(risk_score: int, outstanding: float, dpd: float) -> Dict[str, Any]:
    """
    Baseline policy. Replace with Next-Best-Action model later.
    """
    if risk_score >= 85 and outstanding >= 500000 and dpd >= 90:
        return {"action": "LEGAL_ESCALATION", "priority": "P0", "reason": "High risk + high exposure + high DPD"}
    if risk_score >= 85 and dpd >= 60:
        return {"action": "FIELD_VISIT", "priority": "P1", "reason": "High risk and DPD>=60"}
    if risk_score >= 65:
        return {"action": "CALL_CENTER", "priority": "P2", "reason": "Medium-high risk"}
    return {"action": "SMS_REMINDER", "priority": "P3", "reason": "Low-medium risk"}