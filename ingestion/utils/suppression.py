import logging                                                                            
                                                                                            
# NOTE: This module handles suppression at the individual cell level only.                
# The cascading total behavior — where a suppressed disaggregated value                   
# causes the calculated total column to also be suppressed — must be                      
# handled in load_staging.py at the row level, after handle_suppression()
# has been applied to all individual cells.                                               
#
# Cascading rules by vintage:                                                             
#   2017-18 and 2020-21: if any disaggregated value is -11, total → -11
#   2021-22: if any disaggregated value is -12, total → -12                               
# (Verified empirically against actual OCR data files)
                                                                                            
# IMPORTANT: handle_suppression() should be called on ALL columns except                  
# pure identifier columns (COMBOKEY, LEAID, SCHID, school_name, state,
# etc.). Reserve codes can appear in numeric count columns, boolean/indicator             
# columns, and categorical text columns like yes/no fields. Do NOT skip
# suppression handling for a column just because it contains text values —                
# the only columns to skip are those that serve as unique identifiers and
# could never contain a reserve code by definition.                                       
                  
logger = logging.getLogger(__name__)

RESERVE_CODES = {                                                                         
    "2017-18": {-3, -5, -6, -8, -9, -11},
    "2020-21": {-3, -4, -5, -6, -8, -9, -11, -13},                                        
    "2021-22": {-3, -4, -5, -6, -9, -12, -13},                                            
}
                                                                                            
# These codes should not appear in public-use CRDC SCH files.                             
# -4 is restricted-use only. -8 is EDFacts only.
UNEXPECTED_CODES = {-4, -8}                                                               
                                                                                            
                                                                                            
def handle_suppression(value, vintage):                                                   
    """         
    Given a raw value from a CRDC CSV and the vintage string, return a tuple of:
        (cleaned_value, is_suppressed, suppression_code)                                    
                                                                                            
    Reserve codes are converted to NULL (None) with is_suppressed=True.                   
    Non-numeric values are returned unchanged with is_suppressed=False.                   
    """                                                                                   
    try:        
        numeric = int(float(value))                                                       
    except (ValueError, TypeError):
        return (value, False, None)

    if numeric >= 0:                                                                      
        return (numeric, False, None)
                                                                                            
    known_codes = RESERVE_CODES.get(vintage, set())                                       
   
    if numeric in UNEXPECTED_CODES:                                                       
        logger.warning(
            f"Unexpected reserve code {numeric} encountered for vintage={vintage}. "
            f"Code {numeric} should not appear in public-use CRDC SCH files."             
        )                                                                                 
        return (None, True, numeric)                                                      
                                                                                            
    if numeric not in known_codes:
        logger.warning(
            f"Unknown reserve code {numeric} encountered for vintage={vintage}. "
            f"Known codes for this vintage: {known_codes}. Treating as suppressed."
        )                                                                                 
        return (None, True, numeric)
                                                                                            
    return (None, True, numeric)
