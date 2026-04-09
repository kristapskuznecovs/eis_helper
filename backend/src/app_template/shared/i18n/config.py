from enum import Enum


class Locale(str, Enum):
    LV = "lv"
    EN = "en"


DEFAULT_LOCALE = Locale.LV
SUPPORTED_LOCALES = {locale.value for locale in Locale}
LOCALE_HEADER = "X-Locale"
