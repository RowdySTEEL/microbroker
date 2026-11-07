"""Longer market-data-only research collection; Paper duration/order policy is untouched."""
from dataclasses import dataclass
from .session import SessionRequest,run_session
from .models import DataValidationError


@dataclass(frozen=True)
class EventStudySessionRequest(SessionRequest):
    symbols: tuple=('AAPL','MSFT','NVDA','TSLA')
    duration: int=5400
    poll_seconds: int=30

    def __post_init__(self):
        if type(self.duration) is not int or not 60<=self.duration<=10800:
            raise DataValidationError('Event research duration must be 60..10800 seconds; no orders')
        checked=SessionRequest(self.symbols,self.feed,min(self.duration,3600),self.poll_seconds,self.lookback_minutes)
        news=tuple(s for s in checked.symbols if s!='SPY')
        if not news:raise DataValidationError('Specify at least one company symbol besides SPY')
        object.__setattr__(self,'news_symbols',news)
        object.__setattr__(self,'symbols',tuple(sorted(set(news)|{'SPY'})))

    def metadata(self):
        return {**super().metadata(),'purpose':'event_study','collection_format':'R30',
            'news_symbols':list(self.news_symbols),'price_symbols':list(self.symbols),
            'benchmark':'SPY','orders_enabled':False}


def run_event_collection(settings,request,engine,**kwargs):
    if not isinstance(request,EventStudySessionRequest):raise DataValidationError('Use an event-study session request')
    result=run_session(settings,request,engine,**kwargs)
    result['event_study_collection']={'format':'R30','news_symbols':list(request.news_symbols),
        'price_symbols':list(request.symbols),'benchmark':'SPY','orders_enabled':False,
        'next_command':'event-study --latest --format text',
        'late_events_can_have_unobserved_60_minute_outcomes':True}
    return result
