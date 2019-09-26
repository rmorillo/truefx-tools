from enum import Enum
from collections import namedtuple

class TimeUnit(Enum):
    Minute = 1
    Hour = 2
    Day = 3
    Month = 4


class Timeframe:
    def __init__(self, time_unit, units):
        self.time_unit = time_unit
        self.units = units


class TimeframeUnit(Enum):
    M1 = Timeframe(TimeUnit.Minute, 1)
    M5 = Timeframe(TimeUnit.Minute, 5)
    M10 = Timeframe(TimeUnit.Minute, 10)
    M15 = Timeframe(TimeUnit.Minute, 15)
    M30 = Timeframe(TimeUnit.Minute, 30)
    H1 = Timeframe(TimeUnit.Hour, 1)
    H4 = Timeframe(TimeUnit.Hour, 4)
    D1 = Timeframe(TimeUnit.Day, 1)

    @staticmethod
    def get_timeframe_unit_by_name(name):
        for timeframe in TimeframeUnit:
            if timeframe.name.lower() == name.lower():
                return timeframe
        return None


class Currency(Enum):
    Usd = "USD"
    Eur = "EUR"
    Gbp = "GBP"
    Jpy = "JPY"
    Chf = "CHF"
    Aud = "AUD"
    Nzd = "NZD"
    Cad = "CAD"

class CurrencyPair:
    def __init__(self, currency_pair_id, base_currency, quote_currency, pip_factor = 10000):
        self.currency_pair_id = currency_pair_id
        self.base_currency = base_currency
        self.quote_currency = quote_currency
        self.pip_factor = pip_factor

    @property
    def name(self):
        return self.base_currency.value + self.quote_currency.value

class FxMajor(Enum):
    EurUsd = CurrencyPair(1, Currency.Eur, Currency.Usd)
    GbpUsd = CurrencyPair(2, Currency.Gbp, Currency.Usd)
    UsdJpy = CurrencyPair(3, Currency.Usd, Currency.Jpy, pip_factor=100)
    UsdChf = CurrencyPair(4, Currency.Usd, Currency.Chf)
    AudUsd = CurrencyPair(5, Currency.Aud, Currency.Usd)
    NzdUsd = CurrencyPair(6, Currency.Nzd, Currency.Usd)
    UsdCad = CurrencyPair(7, Currency.Usd, Currency.Cad)

    @staticmethod
    def get_pair_list():
        return [FxMajor.EurUsd, FxMajor.GbpUsd, FxMajor.UsdJpy, FxMajor.UsdChf, FxMajor.AudUsd,
                FxMajor.NzdUsd, FxMajor.UsdCad]

    @staticmethod
    def get_pair_by_symbols(base_currency, quote_currency):
        for pair in FxMajor.get_pair_list():
            if pair.value.base_currency.value.lower() == base_currency.lower() and \
                            pair.value.quote_currency.value.lower() == quote_currency.lower():
                return pair
        return None

    @staticmethod
    def get_pair_by_id(id):
        for pair in FxMajor.get_pair_list():
            if pair.value.currency_pair_id == id:
                return pair
        return None

class CorrelatedPair:
    def __init__(self, correlated_pair_id, x_currency_pair, y_currency_pair, correlation_type):
        self.correlated_pair_id = correlated_pair_id
        self.x_currency_pair = x_currency_pair
        self.y_currency_pair = y_currency_pair
        self.correlation_type = correlation_type

    @property
    def name(self):
        return self.x_currency_pair.value.name + "_" + self.y_currency_pair.value.name

class CorrelationType(Enum):
    NoCorrelation = 0
    Positive = 1
    Negative = -1

    @staticmethod
    def inverse(correlation_type):
        if correlation_type == CorrelationType.Positive:
            return CorrelationType.Negative
        elif correlation_type == CorrelationType.Negative:
            return CorrelationType.Positive
        else:
            CorrelationType.NoCorrelation

class FxCorrelatedPair(Enum):
    EurUsd_GbpUsd = CorrelatedPair(1, FxMajor.EurUsd, FxMajor.GbpUsd, CorrelationType.Positive)
    EurUsd_AudUsd = CorrelatedPair(2, FxMajor.EurUsd, FxMajor.AudUsd, CorrelationType.Positive)
    EurUsd_NzdUsd = CorrelatedPair(3, FxMajor.EurUsd, FxMajor.NzdUsd, CorrelationType.Positive)
    UsdChf_UsdJpy = CorrelatedPair(4, FxMajor.UsdChf, FxMajor.UsdJpy, CorrelationType.Positive)
    AudUsd_NzdUsd = CorrelatedPair(5, FxMajor.AudUsd, FxMajor.NzdUsd, CorrelationType.Positive)
    GbpUsd_UsdJpy = CorrelatedPair(6, FxMajor.GbpUsd, FxMajor.UsdJpy, CorrelationType.Negative)
    EurUsd_UsdChf = CorrelatedPair(7, FxMajor.EurUsd, FxMajor.UsdChf, CorrelationType.Negative)
    GbpUsd_UsdChf = CorrelatedPair(8, FxMajor.GbpUsd, FxMajor.UsdChf, CorrelationType.Negative)
    AudUsd_UsdJpy = CorrelatedPair(9, FxMajor.AudUsd, FxMajor.UsdJpy, CorrelationType.Negative)
    AudUsd_UsdCad = CorrelatedPair(10, FxMajor.AudUsd, FxMajor.UsdCad, CorrelationType.Negative)

    @staticmethod
    def all_correlated_pairs():
        return [FxCorrelatedPair.EurUsd_GbpUsd,
                FxCorrelatedPair.EurUsd_AudUsd,
                FxCorrelatedPair.EurUsd_NzdUsd,
                FxCorrelatedPair.UsdChf_UsdJpy,
                FxCorrelatedPair.AudUsd_NzdUsd,
                FxCorrelatedPair.GbpUsd_UsdJpy,
                FxCorrelatedPair.EurUsd_UsdChf,
                FxCorrelatedPair.GbpUsd_UsdChf,
                FxCorrelatedPair.AudUsd_UsdJpy,
                FxCorrelatedPair.AudUsd_UsdCad
                ]

    @staticmethod
    def get_correlated_pair_by_id(id):
        for pair in FxCorrelatedPair.all_correlated_pairs():
            if pair.value.correlated_pair_id == id:
                return pair
        return None

class SignalType(Enum):
    Entry = 1
    Exit = 2
    Reversal = 3

class CorrelationMode(Enum):
    Unknown = 0
    Normal = 1
    Abnormal = -1

class StructFieldType(Enum):
    Void = ""
    Float = "f"
    Integer = "i"
    Boolean = "?"


EntrySignalColumns = namedtuple("EntrySignalColumns", "instrument price position strength")
ReversalSignalColumns = namedtuple("EntrySignalColumns", "instrument price previous_position current_position strength")
ExitSignalColumns = namedtuple("EntrySignalColumns", "instrument price position")

class Signal:
    def __init__(self, signal_id, timestamp):
        self.signal_id = signal_id
        self.timestamp = timestamp
        self.entry_signals = []
        self.reversal_signals = []
        self.exit_signals = []

    def add_entry_signal(self, instrument, price, position, strength):
        self.entry_signals.append(EntrySignalColumns(instrument, price, position, strength))

    def add_reversal_signal(self, instrument, price, previous_position, current_position, strength):
        self.reversal_signals.append(ReversalSignalColumns(instrument, price, previous_position, current_position, strength))

    def add_exit_signal(self, instrument, price, position):
        self.exit_signals.append(ExitSignalColumns(instrument, price, position))

    @property
    def has_signal(self):
        return (len(self.entry_signals) + len(self.reversal_signals) + len(self.exit_signals)) > 0

    @property
    def has_entry_signal(self):
        return len(self.entry_signals) > 0

    @property
    def has_exit_signal(self):
        return len(self.exit_signals) > 0

    @property
    def has_reversal_signal(self):
        return len(self.exit_signals) > 0

class FxMarketSession:
    Friday = 4
    Sunday = 6
    Weekends = [5, 6]
    Weekdays = [0, 1, 2, 3, 4]

    @staticmethod
    def is_start_of_week_opening(timestamp):
        weekday = timestamp.weekday()
        hour = timestamp.hour
        minute = timestamp.minute
        return (weekday == FxMarketSession.Sunday and hour >= 21 and minute >= 0 and minute <= 59)

    @staticmethod
    def is_end_of_week_market_closing(timestamp):
        return timestamp.weekday() == FxMarketSession.Friday and timestamp.hour >= 18 and timestamp.minute >= 0

    @staticmethod
    def is_end_of_week_cutoff(timestamp):
        return timestamp.weekday() == FxMarketSession.Friday and timestamp.hour >= 20 and timestamp.minute >= 0

    @staticmethod
    def is_market_open(timestamp):
        weekday = timestamp.weekday()
        hour = timestamp.hour
        minute = timestamp.minute
        return weekday in FxMarketSession.Weekdays or (weekday == FxMarketSession.Sunday and hour >= 21 and minute >= 0)\
               or (weekday == FxMarketSession.Friday and hour < 21)

    @staticmethod
    def is_market_close(timestamp):
        weekday = timestamp.weekday()
        hour = timestamp.hour
        minute = timestamp.minute
        return weekday in FxMarketSession.Weekends or (weekday == FxMarketSession.Sunday and hour < 21) \
               or (weekday == FxMarketSession.Friday and hour >= 21 and minute >= 0)

class StopLossHitType(Enum):
    Nothing = 0
    Losing = 1
    BreakEven = 2
    Winning = 3
    Cutoff = 4

    @staticmethod
    def next_hit_type(current_hit_level):
        if current_hit_level==StopLossHitType.Nothing:
            return StopLossHitType.Losing
        elif current_hit_level==StopLossHitType.Losing:
            return StopLossHitType.BreakEven
        elif current_hit_level==StopLossHitType.BreakEven:
            return StopLossHitType.Winning
        elif current_hit_level==StopLossHitType.Winning:
            return None
        else:
            return None

    @staticmethod
    def hit_type_by_value(value):
        if value == 0:
            return StopLossHitType.Nothing
        if value == 1:
            return StopLossHitType.Losing
        elif value == 2:
            return StopLossHitType.BreakEven
        elif value == 3:
            return StopLossHitType.Winning
        elif value == 4:
            return StopLossHitType.Cutoff
        else:
            return None

SignalAdviceColumns = namedtuple("MarketOrderColumns","timestamp currency_pair_id position position_size")