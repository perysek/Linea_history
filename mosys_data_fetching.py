"""
MOSYS Data Fetching Functions
Provides functions to fetch various types of data from the MOSYS/STAAMPDB database.
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from MOSYS_con import get_pervasive


# ============================================================================
# TOOLS MANAGEMENT
# ============================================================================

def get_tool_details(tool_code: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch tool/mold details from STAMPI table.

    Args:
        tool_code: Optional specific tool code to filter (CODICE)

    Returns:
        DataFrame with columns: CODICE, CODICE_PROPRIETARIO, NOME_PROPRIETARIO,
        DESCRIZIONE, FIGURE, PESO, CICLO_STD, SCARTI_PERC, ALTEZZA, LARGHEZZA,
        PROFONDITA, RAPPORTO_MONTAGGIO, COD_STAMPO_CLI, RAPPORTO_SMONTAGGIO,
        MESE_ANNO_COSTRUZ, TIPO_INIEZIONE, CODICE_UBICAZIONE, DESCR_UBICAZIONE, NOTE
    """
    if tool_code:
        query = "SELECT * FROM STAAMPDB.STAMPI WHERE CODICE = ?"
        return get_pervasive(query, (tool_code,))
    else:
        query = "SELECT * FROM STAAMPDB.STAMPI ORDER BY CODICE"
        return get_pervasive(query, ())


def get_tool_relationships(tool_code: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch tool relationships/hierarchy from STAMPI2 table.

    Args:
        tool_code: Optional specific tool code to filter

    Returns:
        DataFrame with columns: CODICE, CODICE_PADRE, CODICE_STAMPO_BASE,
        COSTRUTTORE, LIBERO
    """
    if tool_code:
        query = """
        SELECT * FROM STAAMPDB.STAMPI2
        WHERE CODICE = ? OR CODICE_PADRE = ? OR CODICE_STAMPO_BASE = ?
        """
        return get_pervasive(query, (tool_code, tool_code, tool_code))
    else:
        query = "SELECT * FROM STAAMPDB.STAMPI2 ORDER BY CODICE"
        return get_pervasive(query, ())


def get_tool_location(location_code: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch tools by location.

    Args:
        location_code: Optional location code to filter (CODICE_UBICAZIONE)

    Returns:
        DataFrame with tool details filtered by location
    """
    if location_code:
        query = """
        SELECT CODICE, DESCRIZIONE, CODICE_UBICAZIONE, DESCR_UBICAZIONE, TIPO_INIEZIONE
        FROM STAAMPDB.STAMPI
        WHERE CODICE_UBICAZIONE = ?
        ORDER BY CODICE
        """
        return get_pervasive(query, (location_code,))
    else:
        query = """
        SELECT DISTINCT CODICE_UBICAZIONE, DESCR_UBICAZIONE
        FROM STAAMPDB.STAMPI
        WHERE CODICE_UBICAZIONE IS NOT NULL
        ORDER BY CODICE_UBICAZIONE
        """
        return get_pervasive(query, ())


# ============================================================================
# TOOL REPAIRS
# ============================================================================

def get_tool_repairs(
    tool_code: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    status: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch tool repair records from RIPARAZ table.

    Args:
        tool_code: Optional tool code to filter (CODICE_STAMPO)
        start_date: Optional start date for repairs (DATA_INIZIO)
        end_date: Optional end date for repairs
        status: Optional repair status to filter (STATO_RIPARAZIONE)

    Returns:
        DataFrame with columns: CODICE_STAMPO, COMMESSA, CODICE_RIPARAZIONE,
        DATA_INIZIO, ORA_INIZIO, OPER_INIZIO, STATO_RIPARAZIONE, NOTE01-NOTE10,
        DATA_FINE, ORA_FINE, OPER_FINE, DATA_COLLAUDO, ORA_COLLAUDO,
        OPER_COLLAUDO, FLAG_FARE_CONTROLLI, FLAG_PROVA_URGENTE, NUMERO_NONCONF
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.RIPARAZ WHERE 1=1"

    if tool_code:
        conditions.append("CODICE_STAMPO = ?")
        params.append(tool_code)

    if start_date:
        conditions.append("DATA_INIZIO >= ?")
        params.append(start_date.strftime('%Y-%m-%d'))

    if end_date:
        conditions.append("DATA_INIZIO <= ?")
        params.append(end_date.strftime('%Y-%m-%d'))

    if status:
        conditions.append("STATO_RIPARAZIONE = ?")
        params.append(status)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_INIZIO DESC, ORA_INIZIO DESC"

    return get_pervasive(query, tuple(params))


def get_repair_details(repair_code: str) -> pd.DataFrame:
    """
    Fetch detailed repair information from RIPARAZ1 table.

    Args:
        repair_code: Repair code to fetch (CODICE_RIPARAZIONE)

    Returns:
        DataFrame with columns: CODICE_RIPARAZIONE, NOTE01-NOTE10,
        CODICE_CLIENTE, LIBERO1-LIBERO4
    """
    query = "SELECT * FROM STAAMPDB.RIPARAZ1 WHERE CODICE_RIPARAZIONE = ?"
    return get_pervasive(query, (repair_code,))


def get_active_repairs() -> pd.DataFrame:
    """
    Fetch all currently active repairs (no end date).

    Returns:
        DataFrame with active repair records
    """
    query = """
    SELECT * FROM STAAMPDB.RIPARAZ
    WHERE DATA_FINE IS NULL OR DATA_FINE = ''
    ORDER BY DATA_INIZIO DESC
    """
    return get_pervasive(query, ())


def get_urgent_repairs() -> pd.DataFrame:
    """
    Fetch urgent repairs that need immediate attention.

    Returns:
        DataFrame with urgent repair records
    """
    query = """
    SELECT * FROM STAAMPDB.RIPARAZ
    WHERE FLAG_PROVA_URGENTE = 1 AND (DATA_FINE IS NULL OR DATA_FINE = '')
    ORDER BY DATA_INIZIO
    """
    return get_pervasive(query, ())


# ============================================================================
# TOOL MAINTENANCE
# ============================================================================

def get_maintenance_schedule(
    machine_type: Optional[str] = None,
    machine_code: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch maintenance schedules from MANORD table.

    Args:
        machine_type: Optional machine type to filter (TIPO_MACCHINARIO)
        machine_code: Optional machine code to filter (CODICE_MACCHINARIO)

    Returns:
        DataFrame with columns: TIPO_MACCHINARIO, CODICE_MACCHINARIO,
        NUMERO_MANUTENZIONE, DESCRIZIONE_BREVE, INTERVALLO_GG, DESC01-DESC15,
        DATA_ULTIMA_MAN, DATA_PROSSIMA_MAN, LIBERO
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.MANORD WHERE 1=1"

    if machine_type:
        conditions.append("TIPO_MACCHINARIO = ?")
        params.append(machine_type)

    if machine_code:
        conditions.append("CODICE_MACCHINARIO = ?")
        params.append(machine_code)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_PROSSIMA_MAN"

    return get_pervasive(query, tuple(params))


def get_overdue_maintenance() -> pd.DataFrame:
    """
    Fetch overdue maintenance tasks (past due date).

    Returns:
        DataFrame with overdue maintenance records
    """
    today = datetime.now().strftime('%Y-%m-%d')
    query = """
    SELECT * FROM STAAMPDB.MANORD
    WHERE DATA_PROSSIMA_MAN < ?
    ORDER BY DATA_PROSSIMA_MAN
    """
    return get_pervasive(query, (today,))


def get_upcoming_maintenance(days_ahead: int = 30) -> pd.DataFrame:
    """
    Fetch upcoming maintenance tasks within specified days.

    Args:
        days_ahead: Number of days to look ahead (default 30)

    Returns:
        DataFrame with upcoming maintenance records
    """
    today = datetime.now()
    future_date = (today + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')

    query = """
    SELECT * FROM STAAMPDB.MANORD
    WHERE DATA_PROSSIMA_MAN >= ? AND DATA_PROSSIMA_MAN <= ?
    ORDER BY DATA_PROSSIMA_MAN
    """
    return get_pervasive(query, (today_str, future_date))


def get_scheduled_maintenance_records(
    machine_type: Optional[str] = None,
    machine_code: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> pd.DataFrame:
    """
    Fetch scheduled maintenance execution records from REGMANU table.

    Args:
        machine_type: Optional machine type to filter
        machine_code: Optional machine code to filter
        start_date: Optional start date for maintenance records
        end_date: Optional end date for maintenance records

    Returns:
        DataFrame with columns: TIPO_MACCHINARIO, CODICE_MACCHINARIO,
        NUMERO_MANUTENZIONE, DATA_EFF_MANUT, OPERATORE, DATA_PREV_MANUT,
        NOTE01-NOTE10, LIBERO
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.REGMANU WHERE 1=1"

    if machine_type:
        conditions.append("TIPO_MACCHINARIO = ?")
        params.append(machine_type)

    if machine_code:
        conditions.append("CODICE_MACCHINARIO = ?")
        params.append(machine_code)

    if start_date:
        conditions.append("DATA_EFF_MANUT >= ?")
        params.append(start_date.strftime('%Y-%m-%d'))

    if end_date:
        conditions.append("DATA_EFF_MANUT <= ?")
        params.append(end_date.strftime('%Y-%m-%d'))

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_EFF_MANUT DESC"

    return get_pervasive(query, tuple(params))


def get_unscheduled_maintenance_records(
    machine_type: Optional[str] = None,
    machine_code: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> pd.DataFrame:
    """
    Fetch unscheduled/simple maintenance records from REGMANUS table.

    Args:
        machine_type: Optional machine type to filter
        machine_code: Optional machine code to filter
        start_date: Optional start date for maintenance records
        end_date: Optional end date for maintenance records

    Returns:
        DataFrame with columns: TIPO_MACCHINARIO, CODICE_MACCHINARIO,
        DATA_MANUTENZIONE, OPERATORE, DESCRIZ_BREVE, NOTE01-NOTE10,
        ORE_LAVORATE, LIBERO
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.REGMANUS WHERE 1=1"

    if machine_type:
        conditions.append("TIPO_MACCHINARIO = ?")
        params.append(machine_type)

    if machine_code:
        conditions.append("CODICE_MACCHINARIO = ?")
        params.append(machine_code)

    if start_date:
        conditions.append("DATA_MANUTENZIONE >= ?")
        params.append(start_date.strftime('%Y-%m-%d'))

    if end_date:
        conditions.append("DATA_MANUTENZIONE <= ?")
        params.append(end_date.strftime('%Y-%m-%d'))

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_MANUTENZIONE DESC"

    return get_pervasive(query, tuple(params))


# ============================================================================
# PRODUCTION ORDERS
# ============================================================================

def get_quality_tests(
    test_number: Optional[int] = None,
    press: Optional[str] = None,
    order_number: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> pd.DataFrame:
    """
    Fetch quality test records from COLLAUDO table.

    Args:
        test_number: Optional test number to filter (NUMERO_COLLAUDO)
        press: Optional press code to filter (PRESSA)
        order_number: Optional order number to filter (COMMESSA)
        start_date: Optional start date for tests
        end_date: Optional end date for tests

    Returns:
        DataFrame with columns: NUMERO_COLLAUDO, PRESSA, COMMESSA, ARTICOLO,
        CLIENTE, DISEGNO, OPERATORE_PRESSA, DATA_COLLAUDO, NOTE01-NOTE05,
        OPERATORE_COLLAUDO, FLAG_AUTOCER, ORA_COLLAUDO, FLAG_PROVVISORIO,
        STAMPO_I, STAMPO_P
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.COLLAUDO WHERE 1=1"

    if test_number:
        conditions.append("NUMERO_COLLAUDO = ?")
        params.append(test_number)

    if press:
        conditions.append("PRESSA = ?")
        params.append(press)

    if order_number:
        conditions.append("COMMESSA = ?")
        params.append(order_number)

    if start_date:
        conditions.append("DATA_COLLAUDO >= ?")
        params.append(start_date.strftime('%Y-%m-%d'))

    if end_date:
        conditions.append("DATA_COLLAUDO <= ?")
        params.append(end_date.strftime('%Y-%m-%d'))

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_COLLAUDO DESC, ORA_COLLAUDO DESC"

    return get_pervasive(query, tuple(params))


def get_production_batches(
    order_number: Optional[str] = None,
    press: Optional[str] = None,
    tool_code: Optional[str] = None,
    article: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> pd.DataFrame:
    """
    Fetch production batch records from COLLPROD table.

    Args:
        order_number: Optional order number to filter (COMMESSA)
        press: Optional press code to filter (PRESSA)
        tool_code: Optional tool code to filter (STAMPO)
        article: Optional article code to filter (ARTICOLO)
        start_date: Optional start date for batches
        end_date: Optional end date for batches

    Returns:
        DataFrame with columns: COMMESSA, PRESSA, STAMPO, ARTICOLO,
        NUMERO_COLLAUDO, DATA_CONTROLLO, ORA_CONTROLLO, OPERATORE,
        TIPO_CONTROLLO, ESITO_CONTROLLO, QUANTITA_INDICATIVA,
        STATO_SUB_LOTTO, LIBERO
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.COLLPROD WHERE 1=1"

    if order_number:
        conditions.append("COMMESSA = ?")
        params.append(order_number)

    if press:
        conditions.append("PRESSA = ?")
        params.append(press)

    if tool_code:
        conditions.append("STAMPO = ?")
        params.append(tool_code)

    if article:
        conditions.append("ARTICOLO = ?")
        params.append(article)

    if start_date:
        conditions.append("DATA_CONTROLLO >= ?")
        params.append(start_date.strftime('%Y-%m-%d'))

    if end_date:
        conditions.append("DATA_CONTROLLO <= ?")
        params.append(end_date.strftime('%Y-%m-%d'))

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_CONTROLLO DESC, ORA_CONTROLLO DESC"

    return get_pervasive(query, tuple(params))


def get_production_parameters(
    press: Optional[str] = None,
    tool_code: Optional[str] = None,
    order_number: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch production parameters from PARPROD table.
    Includes temperatures, pressures, cycle times, and production/scrap counts.

    Args:
        press: Optional press code to filter (PRESSA)
        tool_code: Optional tool code to filter (STAMPO)
        order_number: Optional order number to filter (COMMESSA)

    Returns:
        DataFrame with 70 columns including:
        - PRESSA, STAMPO, COMMESSA
        - ULT_TEMP_01-10: Last temperatures
        - ULT_TEMPO_INIEZIONE, ULT_TEMPO_DOSAGGIO, ULT_TEMPO_CICLO
        - ULT_PEZZI: Last parts count
        - ULT_SCARTI: Last scrap count
        - SCARTI_AVVIO: Startup scrap count
        - MIN/MAX/TOT statistics for all parameters
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.PARPROD WHERE 1=1"

    if press:
        conditions.append("PRESSA = ?")
        params.append(press)

    if tool_code:
        conditions.append("STAMPO = ?")
        params.append(tool_code)

    if order_number:
        conditions.append("COMMESSA = ?")
        params.append(order_number)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_AGGIORNAMENTO DESC, ORA_AGGIORNAMENTO DESC"

    return get_pervasive(query, tuple(params))


def get_production_summary(
    press: Optional[str] = None,
    tool_code: Optional[str] = None,
    order_number: Optional[str] = None
) -> pd.DataFrame:
    """
    Get summary of production with parts produced and scrapped.

    Args:
        press: Optional press code to filter
        tool_code: Optional tool code to filter
        order_number: Optional order number to filter

    Returns:
        DataFrame with selected columns: PRESSA, STAMPO, COMMESSA,
        DATA_AGGIORNAMENTO, ULT_PEZZI (parts produced), ULT_SCARTI (parts scrapped),
        SCARTI_AVVIO (startup scrap), ULT_TEMPO_CICLO (cycle time)
    """
    conditions = []
    params = []

    query = """
    SELECT PRESSA, STAMPO, COMMESSA, DATA_AGGIORNAMENTO, ORA_AGGIORNAMENTO,
           ULT_PEZZI, ULT_SCARTI, SCARTI_AVVIO, ULT_TEMPO_CICLO
    FROM STAAMPDB.PARPROD
    WHERE 1=1
    """

    if press:
        conditions.append("PRESSA = ?")
        params.append(press)

    if tool_code:
        conditions.append("STAMPO = ?")
        params.append(tool_code)

    if order_number:
        conditions.append("COMMESSA = ?")
        params.append(order_number)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_AGGIORNAMENTO DESC, ORA_AGGIORNAMENTO DESC"

    return get_pervasive(query, tuple(params))


def get_scrap_analysis(
    press: Optional[str] = None,
    tool_code: Optional[str] = None,
    min_scrap_rate: Optional[float] = None
) -> pd.DataFrame:
    """
    Analyze scrap rates across production runs.

    Args:
        press: Optional press code to filter
        tool_code: Optional tool code to filter
        min_scrap_rate: Optional minimum scrap rate % to filter (e.g., 5.0 for 5%)

    Returns:
        DataFrame with scrap analysis including calculated scrap rate %
    """
    df = get_production_summary(press=press, tool_code=tool_code)

    if not df.empty:
        # Calculate scrap rate percentage
        df['TOTAL_SCARTI'] = df['ULT_SCARTI'] + df['SCARTI_AVVIO']
        df['TOTAL_PEZZI'] = df['ULT_PEZZI'] + df['TOTAL_SCARTI']
        df['SCRAP_RATE_%'] = (df['TOTAL_SCARTI'] / df['TOTAL_PEZZI'] * 100).round(2)

        if min_scrap_rate:
            df = df[df['SCRAP_RATE_%'] >= min_scrap_rate]

    return df


# ============================================================================
# DIMENSIONAL CONTROL
# ============================================================================

def get_dimension_characteristics(
    article_code: Optional[str] = None,
    characteristic_ref: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch dimensional characteristics from SCHEDIM1 table.

    Args:
        article_code: Optional article code to filter (CODICE_ARTICOLO)
        characteristic_ref: Optional characteristic reference to filter (RIF_CARATTERISTICA)

    Returns:
        DataFrame with columns: CODICE_ARTICOLO, RIF_MISURA, RIF_CARATTERISTICA,
        TIPO, DESCRIZIONE, UN_MIS, VALORE_NOMINALE, SEGNO_TOLL_INF, TOLL_INF,
        SEGNO_TOLL_SUP, TOLL_SUP, FLAG_RIMOSSO, MIN_RILEVABILE, MAX_RILEVABILE,
        MIN_RILEVATO, MAX_RILEVATO, FLAG_DA_CERTIF, MIN_ACCETTABILE,
        MAX_ACCETTABILE, FLAG_SPC
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.SCHEDIM1 WHERE FLAG_RIMOSSO = 0"

    if article_code:
        conditions.append("CODICE_ARTICOLO = ?")
        params.append(article_code)

    if characteristic_ref:
        conditions.append("RIF_CARATTERISTICA = ?")
        params.append(characteristic_ref)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY CODICE_ARTICOLO, RIF_MISURA"

    return get_pervasive(query, tuple(params))


def get_new_dimension_checks(
    article_code: Optional[str] = None,
    control_code: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch new dimensional check definitions from NSCHEDIM table.

    Args:
        article_code: Optional article code to filter (CODICE_ARTICOLO)
        control_code: Optional control code to filter (CODICE_CONTROLLO)

    Returns:
        DataFrame with columns: CODICE_ARTICOLO, NUMERO_RIFERIMENTO,
        CODICE_CONTROLLO, DESCRIZIONE, NUMERO_STAMPATE, TIPO_CONTROLLO,
        FREQUENZA_MINUTI, CODICE_STRUMENTO, FLAG_MODO_INSER, UN_MIS,
        VALORE_NOMINALE, DATA_AGGIORNAMENTO, OPERATORE_AGG,
        NUMERO_MISURE_PER_C, FLAG_RIMOSSO, NOTE, LIBERO
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.NSCHEDIM WHERE FLAG_RIMOSSO = 0"

    if article_code:
        conditions.append("CODICE_ARTICOLO = ?")
        params.append(article_code)

    if control_code:
        conditions.append("CODICE_CONTROLLO = ?")
        params.append(control_code)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY CODICE_ARTICOLO, NUMERO_RIFERIMENTO"

    return get_pervasive(query, tuple(params))


def get_characteristics_present(
    order_number: Optional[str] = None,
    press: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> pd.DataFrame:
    """
    Fetch characteristics present/measured from CARPRES table.

    Args:
        order_number: Optional order number to filter (COMMESSA)
        press: Optional press code to filter (PRESSA)
        start_date: Optional start date
        end_date: Optional end date

    Returns:
        DataFrame with columns: TIPO_MANUALE, COMMESSA, NOTE_STAMPO, PRESSA,
        DESC_PRESSA, STAMPO, DESC_ART, NOTE_COMM, STATO, DATA_INIZIO_EFF,
        ORA_INIZIO_EFF, DATA_FINE_EFF, ORA_FINE_EFF, PEZZI_DA_ST, PEZZI_ST,
        T_PROD_EFF, RITARDO, ARTICOLO, LOCAL_IP, LOCAL_HOST_NAME,
        DATA_INIZIO_SCH, LIBERO_1, LIBERO_2, LIBERO_3
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.CARPRES WHERE 1=1"

    if order_number:
        conditions.append("COMMESSA = ?")
        params.append(order_number)

    if press:
        conditions.append("PRESSA = ?")
        params.append(press)

    if start_date:
        conditions.append("DATA_INIZIO_EFF >= ?")
        params.append(start_date.strftime('%Y-%m-%d'))

    if end_date:
        conditions.append("DATA_FINE_EFF <= ?")
        params.append(end_date.strftime('%Y-%m-%d'))

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY DATA_INIZIO_EFF DESC"

    return get_pervasive(query, tuple(params))


def get_characteristics_master(
    char_type: Optional[str] = None,
    char_code: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch characteristics master data from ESPCARMT table.

    Args:
        char_type: Optional characteristic type to filter (TIPO)
        char_code: Optional characteristic code to filter (CODICE)

    Returns:
        DataFrame with columns: NUM_PROGR, TIPO, CODICE, DESCRIZIONE,
        QUANTITA_TOT, PREZZO_UNIT_MEDIO, PREZZO_UNIT_RIF, GIACENZA_RIF,
        GIACENZA_PERIODO, LOCAL_IP, LOCAL_HOST_NAME, LIBERO
    """
    conditions = []
    params = []

    query = "SELECT * FROM STAAMPDB.ESPCARMT WHERE 1=1"

    if char_type:
        conditions.append("TIPO = ?")
        params.append(char_type)

    if char_code:
        conditions.append("CODICE = ?")
        params.append(char_code)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY NUM_PROGR"

    return get_pervasive(query, tuple(params))


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_active_orders() -> pd.DataFrame:
    """
    Get all active production orders (from CARPRES with STATO indicating active).

    Returns:
        DataFrame with active order records
    """
    query = """
    SELECT * FROM STAAMPDB.CARPRES
    WHERE STATO NOT IN ('COMPLETATO', 'CHIUSO', 'ANNULLATO')
    ORDER BY DATA_INIZIO_SCH
    """
    return get_pervasive(query, ())


def get_press_utilization(press_code: str, days_back: int = 30) -> Dict:
    """
    Calculate press utilization statistics for the last N days.

    Args:
        press_code: Press code to analyze
        days_back: Number of days to look back (default 30)

    Returns:
        Dictionary with utilization statistics
    """
    start_date = datetime.now() - timedelta(days=days_back)

    # Get production batches
    batches = get_production_batches(
        press=press_code,
        start_date=start_date
    )

    # Get production parameters
    params = get_production_summary(press=press_code)

    stats = {
        'press_code': press_code,
        'period_days': days_back,
        'total_batches': len(batches),
        'total_parts': params['ULT_PEZZI'].sum() if not params.empty else 0,
        'total_scrap': params['ULT_SCARTI'].sum() if not params.empty else 0,
        'avg_cycle_time': params['ULT_TEMPO_CICLO'].mean() if not params.empty else 0,
        'tools_used': params['STAMPO'].nunique() if not params.empty else 0
    }

    if stats['total_parts'] > 0:
        stats['scrap_rate_%'] = round(stats['total_scrap'] / stats['total_parts'] * 100, 2)
    else:
        stats['scrap_rate_%'] = 0

    return stats


def get_tool_usage_history(tool_code: str, days_back: int = 90) -> pd.DataFrame:
    """
    Get complete usage history for a tool including production, maintenance, and repairs.

    Args:
        tool_code: Tool code to analyze
        days_back: Number of days to look back (default 90)

    Returns:
        DataFrame with combined history sorted by date
    """
    start_date = datetime.now() - timedelta(days=days_back)

    # Get production batches
    production = get_production_batches(tool_code=tool_code, start_date=start_date)
    if not production.empty:
        production['EVENT_TYPE'] = 'PRODUCTION'
        production['EVENT_DATE'] = production['DATA_CONTROLLO']

    # Get repairs
    repairs = get_tool_repairs(tool_code=tool_code, start_date=start_date)
    if not repairs.empty:
        repairs['EVENT_TYPE'] = 'REPAIR'
        repairs['EVENT_DATE'] = repairs['DATA_INIZIO']

    # Combine all events
    events = []
    if not production.empty:
        events.append(production[['EVENT_TYPE', 'EVENT_DATE', 'COMMESSA', 'PRESSA']])
    if not repairs.empty:
        events.append(repairs[['EVENT_TYPE', 'EVENT_DATE', 'CODICE_RIPARAZIONE', 'STATO_RIPARAZIONE']])

    if events:
        combined = pd.concat(events, ignore_index=True, sort=False)
        combined = combined.sort_values('EVENT_DATE', ascending=False)
        return combined

    return pd.DataFrame()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == '__main__':
    print("MOSYS Data Fetching Functions - Examples")
    print("=" * 80)

    # Example 1: Get tool details
    print("\n1. Fetching tool details...")
    tools = get_tool_details()
    print(f"   Found {len(tools)} tools")

    # Example 2: Get active repairs
    print("\n2. Fetching active repairs...")
    repairs = get_active_repairs()
    print(f"   Found {len(repairs)} active repairs")

    # Example 3: Get overdue maintenance
    print("\n3. Fetching overdue maintenance...")
    overdue = get_overdue_maintenance()
    print(f"   Found {len(overdue)} overdue maintenance tasks")

    # Example 4: Get production summary for last 7 days
    print("\n4. Fetching recent production summary...")
    from datetime import datetime, timedelta
    start = datetime.now() - timedelta(days=7)
    production = get_production_summary()
    if not production.empty:
        recent = production[pd.to_datetime(production['DATA_AGGIORNAMENTO']) >= start]
        print(f"   Found {len(recent)} production records in last 7 days")
        if len(recent) > 0:
            total_parts = recent['ULT_PEZZI'].sum()
            total_scrap = recent['ULT_SCARTI'].sum()
            print(f"   Total parts: {total_parts:,}")
            print(f"   Total scrap: {total_scrap:,}")

    print("\n" + "=" * 80)
