import re
import requests

OLLAMA_URL   = "http://2.58.56.243:11434/api/generate"
OLLAMA_MODEL = "llama3.2:3b"

_DEFAULT_PROMPT_STEP0 = (
    "Tu es un assistant SMS. Réponds de façon naturelle, courte et humaine au message reçu. "
    "Maximum 2 phrases. Pas de lien, pas de promo. En français. Pas d'emoji excessif."
)
_DEFAULT_PROMPT_STEP1 = (
    "Tu es un assistant SMS. Conclus la conversation chaleureusement en 1-2 phrases. "
    "Pas de lien dans cette réponse. En français."
)


def generate_reply(received_text: str, step: int = 0, custom_prompt: str = "") -> str | None:
    """
    Génère une réponse IA à un SMS reçu via Ollama.
    step=0 : première réponse (engager)
    step=1 : deuxième réponse (avant lien promo)
    custom_prompt : instructions personnalisées depuis l'UI (remplace le prompt par défaut)
    """
    if custom_prompt.strip():
        system = custom_prompt.strip()
    else:
        system = _DEFAULT_PROMPT_STEP0 if step == 0 else _DEFAULT_PROMPT_STEP1

    prompt = f"{system}\n\nSMS reçu : \"{received_text}\"\nTa réponse :"

    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model":  OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.8,
                    "num_predict": 100,
                },
            },
            timeout=30,
        )
        resp.raise_for_status()
        text = resp.json().get("response", "").strip()
        return _clean(text) if text else None
    except Exception:
        return None


def _clean(text: str) -> str:
    text = text.strip('"\'')
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return " ".join(sentences[:2]).strip()
