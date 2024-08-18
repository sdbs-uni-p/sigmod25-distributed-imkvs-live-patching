from urllib.parse import unquote

def parse_message_id(url: str) -> str:
    url = unquote(url)
    # https://www.postgresql.org/message-id/flat/1661334672.728714027@f473.i.mail.ru
    # We just want to have the last part, i.e. this is the message id.
    message_id = url[url.rfind("/") + 1:]
    return message_id.strip()


