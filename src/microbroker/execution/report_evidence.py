"""Read-only interpretation of sealed cycle, revision and timing evidence."""
from copy import deepcopy
from ..data.storage.repository import StorageError
from ..data.stream_models import canonical_time

MAX_CYCLES=256
MAX_SAMPLES=20


def validate_warning_history(value):
    try:
        if value['format']!='R29':raise ValueError()
        known=value['cycles_with_price_fields'];complete=value['cycles_with_complete_price_fields']
        if type(known) is not int or type(complete) is not int or not 0<=complete<=known<=MAX_CYCLES:raise ValueError()
        field_known=[value[k] for k in ('cycles_with_clock_fields','cycles_with_gap_fields')]
        if any(type(n) is not int or not 0<=n<=known for n in field_known):raise ValueError()
        counts=[value[k] for k in ('cycles_with_clock_flags','cycles_with_gaps','cycles_with_any_warning')]
        if known:
            for n,measured in zip(counts[:2],field_known):
                if (measured and (type(n) is not int or not 0<=n<=measured)) or (not measured and n is not None):raise ValueError()
            if type(counts[2]) is not int or not max(n or 0 for n in counts[:2])<=counts[2]<=sum(n or 0 for n in counts[:2]):raise ValueError()
            expected=True if counts[2] else (False if complete==known else None)
            if value['warning_recorded'] is not expected:raise ValueError()
        elif counts!=[None,None,None] or value['warning_recorded'] is not None:raise ValueError()
        if not isinstance(value['samples'],list) or len(value['samples'])>MAX_SAMPLES or len(value['samples'])>(counts[2] or 0):raise ValueError()
    except (KeyError,TypeError,ValueError):raise StorageError('Invalid recorded price warning history') from None
    return deepcopy(value)


def price_warning_history(research,observation=None):
    cycles=research.get('cycles')
    if cycles is not None and (not isinstance(cycles,list) or len(cycles)>MAX_CYCLES):
        raise StorageError('Invalid saved price cycle history')
    entries=[(i,c['paper']) for i,c in enumerate(cycles or [],1) if isinstance(c,dict) and isinstance(c.get('paper'),dict)]
    source='saved_research_cycles' if entries else 'last_saved_observation_only'
    if not entries and observation:entries=[(None,observation)]
    known=complete=clock_known=gap_known=flag_cycles=gap_cycles=warning_cycles=0;flags=set();gaps=set();samples=[];first=last=None
    for index,entry in entries:
        fields=[]
        for name in ('price_clock_flags','price_gap_symbols'):
            value=entry.get(name)
            if value is not None and (not isinstance(value,list) or len(value)>100 or any(not isinstance(s,str) or len(s)>16 for s in value)):
                raise StorageError('Invalid saved price warning symbols')
            fields.append(sorted(set(value)) if value is not None else None)
        clock,gap=fields
        if clock is None and gap is None:continue
        known+=1;complete+=clock is not None and gap is not None
        clock_known+=clock is not None;gap_known+=gap is not None
        flag_cycles+=bool(clock);gap_cycles+=bool(gap);flags.update(clock or []);gaps.update(gap or [])
        if not clock and not gap:continue
        warning_cycles+=1;at=entry.get('observed_at')
        if at is not None:at=canonical_time(at)
        if first is None:first=at
        last=at
        if len(samples)<MAX_SAMPLES:samples.append({'cycle':index,'observed_at':at,'clock_symbols':clock,'gap_symbols':gap})
    return {'format':'R29','source':source,'cycles_in_saved_history':len(cycles) if cycles is not None else None,
        'cycles_with_price_fields':known,'cycles_with_complete_price_fields':complete,
        'cycles_with_clock_fields':clock_known,'cycles_with_gap_fields':gap_known,
        'cycles_with_clock_flags':flag_cycles if clock_known else None,'cycles_with_gaps':gap_cycles if gap_known else None,
        'cycles_with_any_warning':warning_cycles if known else None,
        'clock_symbols':sorted(flags),'gap_symbols':sorted(gaps),
        'warning_recorded':True if warning_cycles else (False if known and complete==known else None),
        'whole_session_history_available':bool(entries) and source=='saved_research_cycles' and len(entries)==len(cycles) and complete==len(entries),
        'first_warning_observed_at':first,'last_warning_observed_at':last,
        'samples':samples,'sample_limit':MAX_SAMPLES,'samples_truncated':warning_cycles>len(samples),
        'scope':'recorded_cycle_health_not_every_receipt_or_independent_outage_detection',
        'warning_cycles_are_not_independent_incidents':True,'network_required':False}


def coverage_view(coverage):
    if not coverage:return None
    def count(name):
        value=coverage.get(name)
        if type(value) is not int or value<0:raise StorageError('Invalid saved decision coverage count')
        return value
    if coverage.get('format')=='R29':
        missing=count('unassessed_company_rows');prior=count('claims_for_prior_revision');other=count('claims_for_different_analysis')
        unclaimed=count('unclaimed_company_rows');current=count('current_analysis_claim_rows');total=count('company_rows')
        if missing!=unclaimed+prior+other or total!=missing+current:raise StorageError('Saved decision coverage totals disagree')
        return {'format':'R29','semantics':'current_revision_and_analyzer_result_match',
            'available_analyses':count('available_analyses'),'company_rows':total,
            'current_revision_rows_not_assessed':missing,'current_revision_rows_not_assessed_at_least':missing,
            'unclaimed_company_rows':unclaimed,'claims_for_prior_revision':prior,'claims_for_different_analysis':other,
            'current_revision_assessment_complete':missing==0,'details_recorded':True,
            'samples':deepcopy(coverage.get('samples',[])),'source':'sealed_r29_coverage'}
    # R28 measured account/article/symbol claim presence, not current revision assessment.
    old=count('unassessed_company_rows');prior=coverage.get('claims_for_prior_revision')
    if prior is not None and (type(prior) is not int or prior<0):raise StorageError('Invalid legacy revision count')
    minimum=old+(prior or 0)
    if minimum>count('company_rows'):raise StorageError('Legacy coverage lower bound exceeds company rows')
    return {'format':'R29','semantics':'legacy_claim_presence_current_analysis_match_not_fully_recorded',
        'available_analyses':count('available_analyses'),'company_rows':count('company_rows'),
        'current_revision_rows_not_assessed':None,'current_revision_rows_not_assessed_at_least':minimum,
        'unclaimed_company_rows':old,'claims_for_prior_revision':prior,'claims_for_different_analysis':None,
        'current_revision_assessment_complete':False if minimum else None,'details_recorded':False,
        'samples':deepcopy(coverage.get('samples',[])),'source':'sealed_legacy_coverage_lower_bound_only'}


def timing_view(timing):
    if not timing:return None
    value=deepcopy(timing)
    if value.get('format')=='R28' and 'portfolio_capture' in value.get('stages',{}):
        value['stages']['legacy_portfolio_capture_including_slot_checks']=value['stages'].pop('portfolio_capture')
        value['legacy_portfolio_capture_mixed']=True
        value['successful_capture_duration_reconstructed']=False
    return value


def render_coverage(view):
    if not view:return []
    show=lambda v:'kaydedilmemiş' if v is None else str(v)
    lines=[f"Revizyon kapsamı: {view['available_analyses']} hazır analiz / {view['company_rows']} şirket satırı",
        f"Karar kaydı olmayan: {view['unclaimed_company_rows']} | Eski revizyon kararı: {show(view['claims_for_prior_revision'])} | Farklı analiz kararı: {show(view['claims_for_different_analysis'])}"]
    exact=view['current_revision_rows_not_assessed']
    if exact is None:
        lines.append(f"Güncel revizyon/analiz eşleşmesi eski kayıtta tam ölçülmedi; en az {view['current_revision_rows_not_assessed_at_least']} şirket satırı incelenmemiş.")
    else:lines.append(f"Güncel revizyon/analiz için incelenmemiş şirket satırı: {exact}")
    lines.append('Bu sayılar bekle kararı veya kaçırılmış işlem sayısı değildir.')
    lines.append('Eşleşen karar kaydı, emir kapılarının tamamlandığını veya işlem yapıldığını göstermez.')
    return lines
