CLIENTS_MAP = {
    "artroyal_detailing": 1,
    # "eurooptik_ufa": 2,
    # "tabib_ufa": 3,
}

def get_client_id(client_slug: str) -> int:
    if client_slug not in CLIENTS_MAP:
        raise ValueError(f"Неизвестный client_slug='{client_slug}'. Добавь в CLIENTS_MAP.")
    return CLIENTS_MAP[client_slug]
