"""Conservative rejection rules; no pronoun resolution or learned entity roles."""
import re

def ambiguity_reason(text, symbol, aliases):
    names=[r'\$?'+re.escape(symbol),*[re.escape(name) for name in aliases[symbol]]]
    target=r'(?:'+ '|'.join(sorted(names,key=len,reverse=True))+r')(?![A-Za-z0-9])'
    lead=text.lstrip(' \t\"\u201c\u2018')
    if re.match(target+r"(?:\s+(?:Corp|Corporation|Inc|Co|Ltd)\.?)?(?:['\u2019]s)?\s+(?:supplier|vendor|partner|customer|client|competitor)\b",lead,re.I):
        return 'third_party_subject'
    if re.match(r'(?:the|this|that)\s+(?:company|firm|business|group|vendor|supplier)\b|(?:it|its|they|their)\b',lead,re.I):
        return 'unresolved_subject'
    if re.match(target,lead,re.I):return None
    relation=re.search(r'\b(?:agreement|contract|deal|partnership|customer|supplier|capacity|relationship)\b',text,re.I)
    if relation and re.search(r'\b(?:with|from|to)\s+(?:the\s+)?'+target,text,re.I):
        return 'counterparty_role_not_established'
    return None
