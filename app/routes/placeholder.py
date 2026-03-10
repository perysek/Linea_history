"""Placeholder routes for features under development."""
from flask import Blueprint, render_template, jsonify, request, current_app
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from datetime import datetime, timedelta
from app import db
from app.models.sorting_area import DaneRaportu, BrakiDefektyRaportu, Operator, KategoriaZrodlaDanych
from app.utils.excel_sync import sync_new_excel_data
from app.utils.auth_helpers import module_required

placeholder_bp = Blueprint('placeholder', __name__)


@placeholder_bp.route('/wykaz-zablokowanych')
@module_required('glowne')
def wykaz_zablokowanych():
    """Lista zablokowanych detali z MOSYS — dane ładowane przez AJAX."""
    return render_template('placeholder/wykaz_zablokowanych.html')


def _format_blocked_parts(parts):
    """Format date fields on blocked parts list returned from MOSYS."""
    for part in parts:
        if part.get('data_niezgodnosci'):
            part['data_niezgodnosci'] = part['data_niezgodnosci'].strftime('%Y-%m-%d')
        min_date = part.get('data_produkcji_min')
        max_date = part.get('data_produkcji_max')
        if min_date and max_date:
            if min_date == max_date:
                part['produced'] = min_date.strftime('%Y/%m/%d')
            else:
                part['produced'] = f"{min_date.strftime('%Y/%m/%d')} - {max_date.strftime('%Y/%m/%d')}"
        else:
            part['produced'] = '-'
    return parts


@placeholder_bp.route('/api/wykaz-zablokowanych')
@module_required('glowne')
def api_wykaz_zablokowanych():
    """AJAX endpoint for blocked parts with server-side filtering, sorting and pagination."""
    sort_field = request.args.get('sort', 'KOD_DETALU')
    sort_dir = request.args.get('dir', 'asc')
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)

    search = {
        'KOD_DETALU': request.args.get('search_KOD_DETALU', '').lower(),
        'NR_NIEZG':   request.args.get('search_NR_NIEZG', '').lower(),
        'DATA_NIEZG': request.args.get('search_DATA_NIEZG', '').lower(),
        'OPIS_NIEZG': request.args.get('search_OPIS_NIEZG', '').lower(),
    }

    try:
        from MOSYS_data_functions import get_all_blocked_parts
        parts = _format_blocked_parts(get_all_blocked_parts())

        # Apply text filters
        field_map = {
            'KOD_DETALU': 'kod_detalu',
            'NR_NIEZG':   'nr_niezgodnosci',
            'DATA_NIEZG': 'data_niezgodnosci',
            'OPIS_NIEZG': 'opis_niezgodnosci',
        }
        for col, val in search.items():
            if val:
                key = field_map[col]
                parts = [p for p in parts if val in (p.get(key) or '').lower()]

        total_blocked = sum(p['ilosc_zablokowanych'] for p in parts)
        total_count = len(parts)

        # Sort
        sort_key_map = {
            'KOD_DETALU':    'kod_detalu',
            'NR_NIEZG':      'nr_niezgodnosci',
            'DATA_NIEZG':    'data_niezgodnosci',
            'OPIS_NIEZG':    'opis_niezgodnosci',
            'PRODUCED':      'produced',
            'ILOSC_OPAKOWAN':'ilosc_opakowan',
            'ILOSC_ZABL':    'ilosc_zablokowanych',
        }
        sort_key = sort_key_map.get(sort_field, 'kod_detalu')
        numeric_keys = {'ilosc_opakowan', 'ilosc_zablokowanych'}
        reverse = sort_dir == 'desc'

        if sort_key in numeric_keys:
            parts.sort(key=lambda p: p.get(sort_key) or 0, reverse=reverse)
        else:
            parts.sort(key=lambda p: (p.get(sort_key) or '').lower(), reverse=reverse)

        # Paginate
        page = parts[offset:offset + limit]

        parts_data = [{
            'kod':      p.get('kod_detalu') or '',
            'nc':       p.get('nr_niezgodnosci') or '',
            'data':     p.get('data_niezgodnosci') or '',
            'opis':     p.get('opis_niezgodnosci') or '',
            'produced': p.get('produced') or '-',
            'opakowan': p.get('ilosc_opakowan') or 0,
            'ilosc':    p.get('ilosc_zablokowanych') or 0,
        } for p in page]

        return jsonify({
            'success': True,
            'parts': parts_data,
            'total_blocked': total_blocked,
            'pagination': {
                'total':    total_count,
                'limit':    limit,
                'offset':   offset,
                'loaded':   len(parts_data),
                'has_more': offset + limit < total_count,
            }
        })
    except Exception as e:
        print(f"Error in api_wykaz_zablokowanych: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@placeholder_bp.route('/api/wykaz-zablokowanych/by-part')
@module_required('glowne')
def api_wykaz_zablokowanych_by_part():
    """AJAX endpoint: blocked parts grouped by part code (Kod detalu view)."""
    sort_field = request.args.get('sort', 'KOD_DETALU')
    sort_dir = request.args.get('dir', 'asc')
    search_kod = request.args.get('search_KOD_DETALU', '').lower()

    try:
        from MOSYS_data_functions import get_blocked_parts_by_part_code
        parts = get_blocked_parts_by_part_code()

        if search_kod:
            parts = [p for p in parts if search_kod in (p.get('kod_detalu') or '').lower()]

        sort_key_map = {
            'KOD_DETALU':    'kod_detalu',
            'NA_STANIE':     'na_stanie',
            'W_TYM_ZABL':    'w_tym_zabl',
            'W_TYM_DOSTEP':  'w_tym_dostep',
        }
        sort_key = sort_key_map.get(sort_field, 'kod_detalu')
        reverse = sort_dir == 'desc'

        if sort_key == 'kod_detalu':
            parts.sort(key=lambda p: (p.get(sort_key) or '').lower(), reverse=reverse)
        else:
            parts.sort(key=lambda p: p.get(sort_key) or 0, reverse=reverse)

        return jsonify({
            'success': True,
            'parts': parts,
            'total_count': len(parts),
            'total_na_stanie': sum(p['na_stanie'] for p in parts),
            'total_w_tym_zabl': sum(p['w_tym_zabl'] for p in parts),
        })
    except Exception as e:
        print(f"Error in api_wykaz_zablokowanych_by_part: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@placeholder_bp.route('/wykaz-zablokowanych/boxes/<nr_niezgodnosci>')
@module_required('glowne')
def get_blocked_boxes(nr_niezgodnosci):
    """Get box details for a specific NC number."""
    try:
        from MOSYS_data_functions import get_blocked_boxes_details
        boxes = get_blocked_boxes_details(nr_niezgodnosci)
        return jsonify({'success': True, 'boxes': boxes})
    except Exception as e:
        print(f"Error fetching boxes for {nr_niezgodnosci}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@placeholder_bp.route('/dane-selekcji')
@module_required('glowne')
def dane_selekcji():
    """Dashboard view with report table - optimized with pagination and date filters."""

    # Sync new Excel data automatically (with 5-minute cache)
    try:
        sync_result = sync_new_excel_data()
        if sync_result['new_records'] > 0:
            current_app.logger.info(f"Excel sync: imported {sync_result['new_records']} new records")
    except Exception as e:
        current_app.logger.error(f"Excel sync failed: {e}")
        # Continue anyway - don't block page load if sync fails

    # Sorting params
    sort_by = request.args.get('sort', 'data_selekcji')
    order = request.args.get('order', 'desc')

    # Date range filter with smart defaults
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    preset = request.args.get('preset', 'last_month')  # Default to last month

    # Apply preset if no custom dates
    if not date_from and not date_to:
        today = datetime.now().date()
        if preset == 'last_week':
            date_from = today - timedelta(days=7)
        elif preset == 'last_month':
            date_from = today - timedelta(days=30)
        elif preset == 'this_month':
            date_from = today.replace(day=1)
        elif preset == 'previous_month':
            first_of_this_month = today.replace(day=1)
            last_of_prev_month = first_of_this_month - timedelta(days=1)
            date_from = last_of_prev_month.replace(day=1)
            date_to = last_of_prev_month
        elif preset == 'last_quarter':
            date_from = today - timedelta(days=90)
        elif preset == 'this_year':
            date_from = today.replace(month=1, day=1)
        elif preset == 'previous_year':
            date_from = today.replace(year=today.year - 1, month=1, day=1)
            date_to = today.replace(year=today.year - 1, month=12, day=31)
        elif preset == 'last_year':
            date_from = today - timedelta(days=365)
    else:
        # Parse custom date strings
        if date_from:
            date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
        if date_to:
            date_to = datetime.strptime(date_to, '%Y-%m-%d').date()

    # Text filters
    filters = {
        'data_selekcji': request.args.get('filter_data_selekcji', ''),
        'operator': request.args.get('filter_operator', ''),
        'nr_raportu': request.args.get('filter_nr_raportu', ''),
        'nr_niezgodnosci': request.args.get('filter_nr_niezgodnosci', ''),
        'data_nc': request.args.get('filter_data_nc', ''),
        'commessa': request.args.get('filter_commessa', ''),
        'kod_detalu': request.args.get('filter_kod_detalu', ''),
        'opis_niezgodnosci': request.args.get('filter_opis_niezgodnosci', ''),
        'nr_instrukcji': request.args.get('filter_nr_instrukcji', ''),
        'defekt': request.args.get('filter_defekt', ''),
    }

    # Build query with eager loading to avoid N+1
    query = DaneRaportu.query.options(
        joinedload(DaneRaportu.operator).joinedload(Operator.dzial),
        joinedload(DaneRaportu.braki_defekty)
    )

    # Apply date range filter
    if date_from:
        query = query.filter(DaneRaportu.data_selekcji >= date_from)
    if date_to:
        query = query.filter(DaneRaportu.data_selekcji <= date_to)

    # Apply text filters
    if filters['data_selekcji']:
        query = query.filter(DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filters['data_selekcji']}%"))
    if filters['operator']:
        query = query.join(Operator).join(KategoriaZrodlaDanych).filter(
            KategoriaZrodlaDanych.opis_kategorii.ilike(f"%{filters['operator']}%")
        )
    if filters['nr_raportu']:
        query = query.filter(DaneRaportu.nr_raportu.ilike(f"%{filters['nr_raportu']}%"))
    if filters['nr_niezgodnosci']:
        query = query.filter(DaneRaportu.nr_niezgodnosci.ilike(f"%{filters['nr_niezgodnosci']}%"))
    if filters['data_nc']:
        query = query.filter(DaneRaportu.data_niezgodnosci.cast(db.String).ilike(f"%{filters['data_nc']}%"))
    if filters['commessa']:
        query = query.filter(DaneRaportu.nr_zamowienia.ilike(f"%{filters['commessa']}%"))
    if filters['kod_detalu']:
        query = query.filter(DaneRaportu.kod_detalu.ilike(f"%{filters['kod_detalu']}%"))
    if filters['opis_niezgodnosci']:
        query = query.filter(DaneRaportu.opis_niezgodnosci.ilike(f"%{filters['opis_niezgodnosci']}%"))
    if filters['nr_instrukcji']:
        query = query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        query = query.join(BrakiDefektyRaportu).filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    # Apply sorting
    valid_sort_columns = ['data_selekcji', 'nr_raportu', 'nr_niezgodnosci', 'data_niezgodnosci',
                          'nr_zamowienia', 'kod_detalu', 'opis_niezgodnosci', 'nr_instrukcji',
                          'selekcja_na_biezaco', 'ilosc_detali_sprawdzonych', 'czas_pracy',
                          'zalecana_wydajnosc']
    if sort_by in valid_sort_columns:
        column = getattr(DaneRaportu, sort_by)
        if order == 'desc':
            query = query.order_by(column.desc())
        else:
            query = query.order_by(column.asc())
    else:
        query = query.order_by(DaneRaportu.data_selekcji.desc())

    # Pre-compute stats with SQL for efficiency (on filtered data)
    stats_query = db.session.query(
        func.count(DaneRaportu.id),
        func.coalesce(func.sum(DaneRaportu.ilosc_detali_sprawdzonych), 0),
        func.coalesce(func.sum(DaneRaportu.czas_pracy), 0)
    )

    # Apply same date filters to stats
    if date_from:
        stats_query = stats_query.filter(DaneRaportu.data_selekcji >= date_from)
    if date_to:
        stats_query = stats_query.filter(DaneRaportu.data_selekcji <= date_to)

    # Apply same text filters to stats
    if filters['data_selekcji']:
        stats_query = stats_query.filter(DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filters['data_selekcji']}%"))
    if filters['operator']:
        stats_query = stats_query.join(Operator).join(KategoriaZrodlaDanych).filter(
            KategoriaZrodlaDanych.opis_kategorii.ilike(f"%{filters['operator']}%")
        )
    if filters['nr_raportu']:
        stats_query = stats_query.filter(DaneRaportu.nr_raportu.ilike(f"%{filters['nr_raportu']}%"))
    if filters['nr_niezgodnosci']:
        stats_query = stats_query.filter(DaneRaportu.nr_niezgodnosci.ilike(f"%{filters['nr_niezgodnosci']}%"))
    if filters['data_nc']:
        stats_query = stats_query.filter(DaneRaportu.data_niezgodnosci.cast(db.String).ilike(f"%{filters['data_nc']}%"))
    if filters['commessa']:
        stats_query = stats_query.filter(DaneRaportu.nr_zamowienia.ilike(f"%{filters['commessa']}%"))
    if filters['kod_detalu']:
        stats_query = stats_query.filter(DaneRaportu.kod_detalu.ilike(f"%{filters['kod_detalu']}%"))
    if filters['opis_niezgodnosci']:
        stats_query = stats_query.filter(DaneRaportu.opis_niezgodnosci.ilike(f"%{filters['opis_niezgodnosci']}%"))
    if filters['nr_instrukcji']:
        stats_query = stats_query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        stats_query = stats_query.join(BrakiDefektyRaportu).filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    stats_result = stats_query.first()

    # Get total defects with SQL
    defects_query = db.session.query(
        func.coalesce(func.sum(BrakiDefektyRaportu.ilosc), 0)
    ).join(DaneRaportu, BrakiDefektyRaportu.raport_id == DaneRaportu.id)

    # Apply same date filters to defects
    if date_from:
        defects_query = defects_query.filter(DaneRaportu.data_selekcji >= date_from)
    if date_to:
        defects_query = defects_query.filter(DaneRaportu.data_selekcji <= date_to)

    # Apply same text filters to defects
    if filters['data_selekcji']:
        defects_query = defects_query.filter(DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filters['data_selekcji']}%"))
    if filters['operator']:
        defects_query = defects_query.join(Operator).join(KategoriaZrodlaDanych).filter(
            KategoriaZrodlaDanych.opis_kategorii.ilike(f"%{filters['operator']}%")
        )
    if filters['nr_raportu']:
        defects_query = defects_query.filter(DaneRaportu.nr_raportu.ilike(f"%{filters['nr_raportu']}%"))
    if filters['nr_niezgodnosci']:
        defects_query = defects_query.filter(DaneRaportu.nr_niezgodnosci.ilike(f"%{filters['nr_niezgodnosci']}%"))
    if filters['data_nc']:
        defects_query = defects_query.filter(DaneRaportu.data_niezgodnosci.cast(db.String).ilike(f"%{filters['data_nc']}%"))
    if filters['commessa']:
        defects_query = defects_query.filter(DaneRaportu.nr_zamowienia.ilike(f"%{filters['commessa']}%"))
    if filters['kod_detalu']:
        defects_query = defects_query.filter(DaneRaportu.kod_detalu.ilike(f"%{filters['kod_detalu']}%"))
    if filters['opis_niezgodnosci']:
        defects_query = defects_query.filter(DaneRaportu.opis_niezgodnosci.ilike(f"%{filters['opis_niezgodnosci']}%"))
    if filters['nr_instrukcji']:
        defects_query = defects_query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        defects_query = defects_query.filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    total_defects = defects_query.scalar() or 0

    # Calculate averages
    avg_scrap_rate = 0
    avg_productivity = 0

    if stats_result[0] > 0:  # If there are any reports
        # Average scrap rate = (total defects / total parts checked) * 100
        if stats_result[1] > 0:
            avg_scrap_rate = (total_defects / stats_result[1]) * 100

        # Average productivity = total parts checked / total hours worked
        if stats_result[2] > 0:
            avg_productivity = stats_result[1] / stats_result[2]

    stats = {
        'count': stats_result[0],
        'parts_checked': stats_result[1],
        'hours_worked': stats_result[2],
        'total_defects': total_defects,
        'average_scrap_rate': avg_scrap_rate,
        'average_productivity': avg_productivity
    }

    # Get all results (no pagination limit for scrolling view)
    reports = query.all()

    # Lazy load missing MOSYS data (data_niezgodnosci OR opis_niezgodnosci absent)
    reports_needing_mosys = [r for r in reports
                             if r.nr_niezgodnosci and (
                                 r.data_niezgodnosci is None or not r.opis_niezgodnosci
                             )]

    if reports_needing_mosys:
        try:
            from MOSYS_data_functions import get_batch_niezgodnosc_details
            nr_list = [r.nr_niezgodnosci for r in reports_needing_mosys]
            mosys_data = get_batch_niezgodnosc_details(nr_list)

            for report in reports_needing_mosys:
                if report.nr_niezgodnosci in mosys_data:
                    data = mosys_data[report.nr_niezgodnosci]
                    report.data_niezgodnosci = data.get('data_niezgodnosci')
                    report.nr_zamowienia = data.get('nr_zamowienia')
                    report.kod_detalu = data.get('kod_detalu')
                    report.opis_niezgodnosci = data.get('opis_niezgodnosci', '')

            db.session.commit()
        except Exception as e:
            print(f"MOSYS lazy load error: {e}")
            db.session.rollback()

    return render_template(
        'placeholder/dane_selekcji.html',
        reports=reports,
        stats=stats,
        sort_by=sort_by,
        order=order,
        filters=filters,
        preset=preset,
        date_from=date_from.isoformat() if isinstance(date_from, datetime) or hasattr(date_from, 'isoformat') else '',
        date_to=date_to.isoformat() if isinstance(date_to, datetime) or hasattr(date_to, 'isoformat') else ''
    )


@placeholder_bp.route('/api/dane-selekcji')
@module_required('glowne')
def api_dane_selekcji():
    """AJAX endpoint for real-time filtering without page reload."""

    # Sorting params
    sort_by = request.args.get('sort', 'data_selekcji')
    order = request.args.get('order', 'desc')

    # Date range filter with smart defaults
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    preset = request.args.get('preset', 'last_month')

    # Apply preset if no custom dates
    if not date_from and not date_to:
        today = datetime.now().date()
        if preset == 'last_week':
            date_from = today - timedelta(days=7)
        elif preset == 'last_month':
            date_from = today - timedelta(days=30)
        elif preset == 'this_month':
            date_from = today.replace(day=1)
        elif preset == 'previous_month':
            first_of_this_month = today.replace(day=1)
            last_of_prev_month = first_of_this_month - timedelta(days=1)
            date_from = last_of_prev_month.replace(day=1)
            date_to = last_of_prev_month
        elif preset == 'last_quarter':
            date_from = today - timedelta(days=90)
        elif preset == 'this_year':
            date_from = today.replace(month=1, day=1)
        elif preset == 'previous_year':
            date_from = today.replace(year=today.year - 1, month=1, day=1)
            date_to = today.replace(year=today.year - 1, month=12, day=31)
        elif preset == 'last_year':
            date_from = today - timedelta(days=365)
    else:
        if date_from:
            date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
        if date_to:
            date_to = datetime.strptime(date_to, '%Y-%m-%d').date()

    # Text filters
    filters = {
        'data_selekcji': request.args.get('filter_data_selekcji', ''),
        'operator': request.args.get('filter_operator', ''),
        'nr_raportu': request.args.get('filter_nr_raportu', ''),
        'nr_niezgodnosci': request.args.get('filter_nr_niezgodnosci', ''),
        'data_nc': request.args.get('filter_data_nc', ''),
        'commessa': request.args.get('filter_commessa', ''),
        'kod_detalu': request.args.get('filter_kod_detalu', ''),
        'opis_niezgodnosci': request.args.get('filter_opis_niezgodnosci', ''),
        'nr_instrukcji': request.args.get('filter_nr_instrukcji', ''),
        'defekt': request.args.get('filter_defekt', ''),
    }

    # Build query with eager loading
    query = DaneRaportu.query.options(
        joinedload(DaneRaportu.operator).joinedload(Operator.dzial),
        joinedload(DaneRaportu.braki_defekty)
    )

    # Apply date range filter
    if date_from:
        query = query.filter(DaneRaportu.data_selekcji >= date_from)
    if date_to:
        query = query.filter(DaneRaportu.data_selekcji <= date_to)

    # Apply text filters
    if filters['data_selekcji']:
        query = query.filter(DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filters['data_selekcji']}%"))
    if filters['operator']:
        query = query.join(Operator).join(KategoriaZrodlaDanych).filter(
            KategoriaZrodlaDanych.opis_kategorii.ilike(f"%{filters['operator']}%")
        )
    if filters['nr_raportu']:
        query = query.filter(DaneRaportu.nr_raportu.ilike(f"%{filters['nr_raportu']}%"))
    if filters['nr_niezgodnosci']:
        query = query.filter(DaneRaportu.nr_niezgodnosci.ilike(f"%{filters['nr_niezgodnosci']}%"))
    if filters['data_nc']:
        query = query.filter(DaneRaportu.data_niezgodnosci.cast(db.String).ilike(f"%{filters['data_nc']}%"))
    if filters['commessa']:
        query = query.filter(DaneRaportu.nr_zamowienia.ilike(f"%{filters['commessa']}%"))
    if filters['kod_detalu']:
        query = query.filter(DaneRaportu.kod_detalu.ilike(f"%{filters['kod_detalu']}%"))
    if filters['opis_niezgodnosci']:
        query = query.filter(DaneRaportu.opis_niezgodnosci.ilike(f"%{filters['opis_niezgodnosci']}%"))
    if filters['nr_instrukcji']:
        query = query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        query = query.join(BrakiDefektyRaportu).filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    # Apply sorting
    valid_sort_columns = ['data_selekcji', 'nr_raportu', 'nr_niezgodnosci', 'data_niezgodnosci',
                          'nr_zamowienia', 'kod_detalu', 'opis_niezgodnosci', 'nr_instrukcji',
                          'selekcja_na_biezaco', 'ilosc_detali_sprawdzonych', 'czas_pracy',
                          'zalecana_wydajnosc']
    if sort_by in valid_sort_columns:
        column = getattr(DaneRaportu, sort_by)
        if order == 'desc':
            query = query.order_by(column.desc())
        else:
            query = query.order_by(column.asc())
    else:
        query = query.order_by(DaneRaportu.data_selekcji.desc())

    # Pre-compute stats with SQL
    stats_query = db.session.query(
        func.count(DaneRaportu.id),
        func.coalesce(func.sum(DaneRaportu.ilosc_detali_sprawdzonych), 0),
        func.coalesce(func.sum(DaneRaportu.czas_pracy), 0)
    )

    # Apply same date filters to stats
    if date_from:
        stats_query = stats_query.filter(DaneRaportu.data_selekcji >= date_from)
    if date_to:
        stats_query = stats_query.filter(DaneRaportu.data_selekcji <= date_to)

    # Apply same text filters to stats
    if filters['data_selekcji']:
        stats_query = stats_query.filter(DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filters['data_selekcji']}%"))
    if filters['operator']:
        stats_query = stats_query.join(Operator).join(KategoriaZrodlaDanych).filter(
            KategoriaZrodlaDanych.opis_kategorii.ilike(f"%{filters['operator']}%")
        )
    if filters['nr_raportu']:
        stats_query = stats_query.filter(DaneRaportu.nr_raportu.ilike(f"%{filters['nr_raportu']}%"))
    if filters['nr_niezgodnosci']:
        stats_query = stats_query.filter(DaneRaportu.nr_niezgodnosci.ilike(f"%{filters['nr_niezgodnosci']}%"))
    if filters['data_nc']:
        stats_query = stats_query.filter(DaneRaportu.data_niezgodnosci.cast(db.String).ilike(f"%{filters['data_nc']}%"))
    if filters['commessa']:
        stats_query = stats_query.filter(DaneRaportu.nr_zamowienia.ilike(f"%{filters['commessa']}%"))
    if filters['kod_detalu']:
        stats_query = stats_query.filter(DaneRaportu.kod_detalu.ilike(f"%{filters['kod_detalu']}%"))
    if filters['opis_niezgodnosci']:
        stats_query = stats_query.filter(DaneRaportu.opis_niezgodnosci.ilike(f"%{filters['opis_niezgodnosci']}%"))
    if filters['nr_instrukcji']:
        stats_query = stats_query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        stats_query = stats_query.join(BrakiDefektyRaportu).filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    stats_result = stats_query.first()

    # Get total defects with SQL
    defects_query = db.session.query(
        func.coalesce(func.sum(BrakiDefektyRaportu.ilosc), 0)
    ).join(DaneRaportu, BrakiDefektyRaportu.raport_id == DaneRaportu.id)

    # Apply same date filters to defects
    if date_from:
        defects_query = defects_query.filter(DaneRaportu.data_selekcji >= date_from)
    if date_to:
        defects_query = defects_query.filter(DaneRaportu.data_selekcji <= date_to)

    # Apply same text filters to defects
    if filters['data_selekcji']:
        defects_query = defects_query.filter(DaneRaportu.data_selekcji.cast(db.String).ilike(f"%{filters['data_selekcji']}%"))
    if filters['operator']:
        defects_query = defects_query.join(Operator).join(KategoriaZrodlaDanych).filter(
            KategoriaZrodlaDanych.opis_kategorii.ilike(f"%{filters['operator']}%")
        )
    if filters['nr_raportu']:
        defects_query = defects_query.filter(DaneRaportu.nr_raportu.ilike(f"%{filters['nr_raportu']}%"))
    if filters['nr_niezgodnosci']:
        defects_query = defects_query.filter(DaneRaportu.nr_niezgodnosci.ilike(f"%{filters['nr_niezgodnosci']}%"))
    if filters['data_nc']:
        defects_query = defects_query.filter(DaneRaportu.data_niezgodnosci.cast(db.String).ilike(f"%{filters['data_nc']}%"))
    if filters['commessa']:
        defects_query = defects_query.filter(DaneRaportu.nr_zamowienia.ilike(f"%{filters['commessa']}%"))
    if filters['kod_detalu']:
        defects_query = defects_query.filter(DaneRaportu.kod_detalu.ilike(f"%{filters['kod_detalu']}%"))
    if filters['opis_niezgodnosci']:
        defects_query = defects_query.filter(DaneRaportu.opis_niezgodnosci.ilike(f"%{filters['opis_niezgodnosci']}%"))
    if filters['nr_instrukcji']:
        defects_query = defects_query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        defects_query = defects_query.filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    total_defects = defects_query.scalar() or 0

    # Calculate averages
    avg_scrap_rate = 0
    avg_productivity = 0

    if stats_result[0] > 0:
        if stats_result[1] > 0:
            avg_scrap_rate = (total_defects / stats_result[1]) * 100
        if stats_result[2] > 0:
            avg_productivity = stats_result[1] / stats_result[2]

    # Get total count for pagination (BEFORE applying limit/offset)
    total_count = query.count()

    # Pagination params
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)

    # Apply pagination and fetch only visible rows
    reports = query.limit(limit).offset(offset).all()

    # Format reports for JSON
    reports_data = []
    for report in reports:
        # Calculate scrap percentage for this report
        scrap_percentage = 0
        if report.ilosc_detali_sprawdzonych > 0:
            scrap_percentage = (report.total_defects / report.ilosc_detali_sprawdzonych) * 100

        # Get defects list
        defects_list = ', '.join([d.defekt for d in report.braki_defekty]) if report.braki_defekty else '-'

        reports_data.append({
            'data_selekcji': report.data_selekcji.strftime('%d.%m.%y') if report.data_selekcji else '-',
            'dzial': report.operator.dzial.opis_kategorii if report.operator and report.operator.dzial else '-',
            'nr_raportu': report.nr_raportu,
            'nr_niezgodnosci': report.nr_niezgodnosci,
            'data_niezgodnosci': report.data_niezgodnosci.strftime('%d.%m.%y') if report.data_niezgodnosci else '-',
            'nr_zamowienia': report.nr_zamowienia or '-',
            'kod_detalu': report.kod_detalu or '-',
            'opis_niezgodnosci': report.opis_niezgodnosci or '-',
            'nr_instrukcji': report.nr_instrukcji or '-',
            'selekcja_na_biezaco': report.selekcja_na_biezaco,
            'ilosc_detali_sprawdzonych': report.ilosc_detali_sprawdzonych,
            'total_defects': report.total_defects,
            'defekty': defects_list,
            'scrap_percentage': scrap_percentage,
            'czas_pracy': report.czas_pracy,
            'rzeczywista_wydajnosc': report.rzeczywista_wydajnosc
        })

    return jsonify({
        'success': True,
        'stats': {
            'count': stats_result[0],
            'parts_checked': stats_result[1],
            'hours_worked': stats_result[2],
            'total_defects': total_defects,
            'average_scrap_rate': avg_scrap_rate,
            'average_productivity': avg_productivity
        },
        'reports': reports_data,
        'pagination': {
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'loaded': len(reports_data),
            'has_more': offset + limit < total_count
        },
        'sort_by': sort_by,
        'order': order
    })


@placeholder_bp.route('/api/nc-history/<nr_niezgodnosci>')
@module_required('glowne')
def api_nc_history(nr_niezgodnosci):
    """AJAX endpoint: NC history entries + blocked parts summary for a given NC number."""
    try:
        from MOSYS_data_functions import (
            get_nc_history,
            get_batch_niezgodnosc_details,
            get_all_blocked_parts,
        )

        # 1. NC history entries
        history = get_nc_history(nr_niezgodnosci)
        history_data = []
        for entry in history:
            data_wpisu = entry.get('data_wpisu')
            history_data.append({
                'data_wpisu': data_wpisu.strftime('%d.%m.%Y') if data_wpisu else '-',
                'godzina_wpisu': entry.get('godzina_wpisu') or '-',
                'tekst_wpisu': entry.get('tekst_wpisu') or '',
                'typ_uwagi': entry.get('typ_uwagi') or '',
            })

        # 2. Resolve kod_detalu (part code) for this NC
        details = get_batch_niezgodnosc_details([nr_niezgodnosci])
        nc_detail = details.get(nr_niezgodnosci, {})
        kod_detalu = nc_detail.get('kod_detalu') or ''

        # 3. All blocked NCs for the same part code
        nr_zamowienia = nc_detail.get('nr_zamowienia') or ''
        related_blocked = []
        if kod_detalu:
            all_blocked = get_all_blocked_parts()
            for part in all_blocked:
                if (part.get('kod_detalu') or '').strip() == kod_detalu.strip():
                    data_nc = part.get('data_niezgodnosci')
                    related_blocked.append({
                        'nr_niezgodnosci': str(part.get('nr_niezgodnosci') or ''),
                        'data_niezgodnosci': data_nc.strftime('%d.%m.%Y') if data_nc else '-',
                        'opis_niezgodnosci': part.get('opis_niezgodnosci') or '-',
                        'ilosc_zablokowanych': part.get('ilosc_zablokowanych') or 0,
                    })
            related_blocked.sort(key=lambda x: x['nr_niezgodnosci'], reverse=True)

        # 4. Sorting activity summary from SQLite grouped by NC for this nr_zamowienia
        sorting_rows = []
        sorting_sum_sorted = 0
        sorting_sum_nok = 0
        sorting_sum_hours = 0.0

        if nr_zamowienia:
            reports = DaneRaportu.query.options(
                joinedload(DaneRaportu.braki_defekty)
            ).filter(DaneRaportu.nr_zamowienia == nr_zamowienia).all()

            # Group by nr_niezgodnosci, accumulate per-NC totals
            nc_groups = {}
            for r in reports:
                nc_key = r.nr_niezgodnosci or '-'
                if nc_key not in nc_groups:
                    nc_groups[nc_key] = {
                        'nr_niezgodnosci': nc_key,
                        'data_niezgodnosci': r.data_niezgodnosci,
                        'total_sorted': 0,
                        'total_nok': 0,
                        'total_hours': 0.0,
                    }
                g = nc_groups[nc_key]
                g['total_sorted'] += r.ilosc_detali_sprawdzonych or 0
                g['total_nok'] += r.total_defects
                g['total_hours'] += r.czas_pracy or 0.0

            for g in sorted(nc_groups.values(), key=lambda x: x['nr_niezgodnosci'], reverse=True):
                scrap = (g['total_nok'] / g['total_sorted'] * 100) if g['total_sorted'] > 0 else None
                wydajnosc = (g['total_sorted'] / g['total_hours']) if g['total_hours'] > 0 else None
                data_nc = g['data_niezgodnosci']
                sorting_rows.append({
                    'nr_niezgodnosci': g['nr_niezgodnosci'],
                    'data_niezgodnosci': data_nc.strftime('%d.%m.%Y') if data_nc else '-',
                    'total_sorted': g['total_sorted'],
                    'total_nok': g['total_nok'],
                    'scrap_rate': round(scrap, 1) if scrap is not None else None,
                    'wydajnosc': round(wydajnosc) if wydajnosc is not None else None,
                })
                sorting_sum_sorted += g['total_sorted']
                sorting_sum_nok += g['total_nok']
                sorting_sum_hours += g['total_hours']

        sorting_summary = {
            'total_sorted': sorting_sum_sorted,
            'total_nok': sorting_sum_nok,
            'scrap_rate': round(sorting_sum_nok / sorting_sum_sorted * 100, 1) if sorting_sum_sorted > 0 else None,
            'wydajnosc': round(sorting_sum_sorted / sorting_sum_hours) if sorting_sum_hours > 0 else None,
        }

        return jsonify({
            'success': True,
            'nr_niezgodnosci': nr_niezgodnosci,
            'history': history_data,
            'blocked': {
                'kod_detalu': kod_detalu,
                'related_ncs': related_blocked,
                'total_blocked_for_pn': sum(r['ilosc_zablokowanych'] for r in related_blocked),
            },
            'sorting': {
                'nr_zamowienia': nr_zamowienia,
                'rows': sorting_rows,
                'summary': sorting_summary,
            },
        })
    except Exception as e:
        print(f"Error fetching NC history for {nr_niezgodnosci}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@placeholder_bp.route('/analiza-danych')
@module_required('analiza')
def analiza_danych():
    """Analiza danych — top NC groups from NOTCOJAN keyword classification."""
    import re as _re
    from MOSYS_data_functions import get_all_notcojan_for_analysis, NC_KEYWORD_CATEGORIES

    error_msg = None
    categories_top10 = []
    total_nc_count = 0

    try:
        all_ncs = get_all_notcojan_for_analysis()
        total_nc_count = len(all_ncs)

        # Compile patterns once
        compiled = [
            {'name': c['name'], 'color': c['color'], 'rx': _re.compile(c['pattern'], _re.IGNORECASE)}
            for c in NC_KEYWORD_CATEGORIES
        ]

        # Count per category
        counts = {c['name']: 0 for c in NC_KEYWORD_CATEGORIES}
        for nc in all_ncs:
            text = nc.get('notes_text', '')
            for cat in compiled:
                if cat['rx'].search(text):
                    counts[cat['name']] += 1
                    break

        # Build sorted category list (desc by count), take top 10
        cat_color = {c['name']: c['color'] for c in NC_KEYWORD_CATEGORIES}
        sorted_cats = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        categories_top10 = [
            {'name': name, 'count': count, 'color': cat_color[name]}
            for name, count in sorted_cats[:10]
            if count > 0
        ]

    except Exception as e:
        error_msg = str(e)

    return render_template(
        'placeholder/analiza_danych.html',
        categories_top10=categories_top10,
        total_nc_count=total_nc_count,
        error_msg=error_msg,
    )


@placeholder_bp.route('/api/nc-category-details/<path:category_name>')
@module_required('analiza')
def api_nc_category_details(category_name):
    """AJAX: per-NC details for a given keyword category."""
    import re as _re
    from MOSYS_data_functions import get_all_notcojan_for_analysis, NC_KEYWORD_CATEGORIES

    # Validate category_name
    cat_cfg = next((c for c in NC_KEYWORD_CATEGORIES if c['name'] == category_name), None)
    if cat_cfg is None:
        return jsonify({'success': False, 'error': 'Category not found'}), 404

    try:
        all_ncs = get_all_notcojan_for_analysis()

        # Compile all patterns for first-match classification
        compiled = [
            {'name': c['name'], 'color': c['color'], 'rx': _re.compile(c['pattern'], _re.IGNORECASE)}
            for c in NC_KEYWORD_CATEGORIES
        ]

        # Collect NCs belonging to this category
        matched_ncs = []
        for nc in all_ncs:
            text = nc.get('notes_text', '')
            for cat in compiled:
                if cat['rx'].search(text):
                    if cat['name'] == category_name:
                        matched_ncs.append(nc)
                    break  # first-match wins

        nr_list = [nc['nr_niezgodnosci'] for nc in matched_ncs]

        # Batch blocked qty — single IN query on SEGCONF+MAGCONF
        blocked_map = {}
        if nr_list:
            try:
                from MOSYS_data_functions import get_pervasive
                placeholders = ','.join(['?' for _ in nr_list])
                q_blocked = f'''
                    SELECT SEGCONF.NUMERO_NON_CONF,
                           SUM(MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) AS TOTAL
                    FROM STAAMPDB.SEGCONF SEGCONF
                    INNER JOIN STAAMPDB.MAGCONF MAGCONF
                        ON SEGCONF.NUMERO_CONFEZIONE = MAGCONF.NUMERO_CONFEZIONE
                    WHERE SEGCONF.NUMERO_NON_CONF IN ({placeholders})
                      AND (MAGCONF.QT_CONTENUTA - MAGCONF.QT_PRELEV) > 0
                    GROUP BY SEGCONF.NUMERO_NON_CONF
                '''
                df_blocked = get_pervasive(q_blocked, tuple(nr_list))
                for _, row in df_blocked.iterrows():
                    key = str(row['NUMERO_NON_CONF']).strip()
                    blocked_map[key] = int(row['TOTAL']) if row['TOTAL'] else 0
            except Exception as e:
                print(f"[api_nc_category_details] blocked qty error: {e}")

        # Sorting summary — batch from SQLite
        sorting_map = {}
        if nr_list:
            try:
                reports = DaneRaportu.query.options(
                    joinedload(DaneRaportu.braki_defekty)
                ).filter(DaneRaportu.nr_niezgodnosci.in_(nr_list)).all()

                for r in reports:
                    key = r.nr_niezgodnosci or ''
                    if key not in sorting_map:
                        sorting_map[key] = {'qty_checked': 0, 'qty_nok': 0}
                    sorting_map[key]['qty_checked'] += r.ilosc_detali_sprawdzonych or 0
                    sorting_map[key]['qty_nok'] += r.total_defects
            except Exception as e:
                print(f"[api_nc_category_details] SQLite sorting error: {e}")

        # Determine open/closed: no blocked qty → treat as closed
        ncs_out = []
        for nc in matched_ncs:
            nr = nc['nr_niezgodnosci']
            blocked_qty = blocked_map.get(nr, 0)
            is_open = blocked_qty > 0

            srt = sorting_map.get(nr)
            if srt and srt['qty_checked'] > 0:
                scrap_rate = round(srt['qty_nok'] / srt['qty_checked'] * 100, 1)
                sorting_out = {
                    'qty_checked': srt['qty_checked'],
                    'qty_nok': srt['qty_nok'],
                    'scrap_rate': scrap_rate,
                }
            else:
                sorting_out = None

            data_nc = nc.get('data_nc')
            ncs_out.append({
                'nr_niezgodnosci': nr,
                'commessa': nc.get('commessa', ''),
                'data_nc': data_nc.strftime('%d.%m.%Y') if data_nc else '-',
                'notes_text': nc.get('notes_text', ''),
                'blocked_qty': blocked_qty,
                'is_open': is_open,
                'sorting': sorting_out,
            })

        # Sort: open first, then by nr_niezgodnosci desc within each group
        ncs_out.sort(key=lambda x: (0 if x['is_open'] else 1, [-ord(c) for c in x['nr_niezgodnosci']]))

        return jsonify({
            'success': True,
            'category': cat_cfg['name'],
            'color': cat_cfg['color'],
            'count': len(ncs_out),
            'ncs': ncs_out,
        })

    except Exception as e:
        print(f"[api_nc_category_details] error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@placeholder_bp.route('/dane-zamowien')
@module_required('analiza')
def dane_zamowien():
    """Dane zamówień produkcyjnych - under construction."""
    return render_template('placeholder/dane_zamowien.html')


@placeholder_bp.route('/utrzymanie-form')
@module_required('zarzadzanie')
def utrzymanie_form():
    """Utrzymanie form - under construction."""
    return render_template('placeholder/utrzymanie_form.html')


@placeholder_bp.route('/kontrola-jakosci')
@module_required('zarzadzanie')
def kontrola_jakosci():
    """Kontrola jakości - under construction."""
    return render_template('placeholder/kontrola_jakosci.html')

KOLUMN_ETYKIETY = {
    'DATA_RILEVAMENTO': 'Data pomiaru',
    'ORA_RILEVAMENTO': 'Godzina pomiaru',
    'DESCRIZIONE': 'Opis charakterystyki',
    'NUMERO_STAMPATA': 'Wtrysk',
    'NUMERO_FIGURA': 'Nr gniazda',
    'MIS01': 'Pomiar 1',
    'MIS02': 'Pomiar 2',
    'MIS03': 'Pomiar 3',
    'MIS04': 'Pomiar 4',
    'MIS05': 'Pomiar 5',
    'MIS06': 'Pomiar 6',
    'MIS07': 'Pomiar 7',
    'MIS08': 'Pomiar 8',
    'MIS09': 'Pomiar 9',
    'MIS10': 'Pomiar 10',
}

_TABLE_COLUMNS = [
    'DATA_RILEVAMENTO', 'ORA_RILEVAMENTO', 'DESCRIZIONE',
    'NUMERO_STAMPATA', 'NUMERO_FIGURA',
    'MIS01', 'MIS02', 'MIS03', 'MIS04', 'MIS05',
    'MIS06', 'MIS07', 'MIS08', 'MIS09', 'MIS10',
]

_RESULTS_COLUMNS = [
    'MIS01', 'MIS02', 'MIS03', 'MIS04', 'MIS05',
    'MIS06', 'MIS07', 'MIS08', 'MIS09', 'MIS10',
]


def _build_nrildim_query(articolo, numero_riferimento, date_from, date_to):
    """Build the main NRILDIM + NSCHEDIM query and params tuple."""
    import pandas as pd
    parts = [
        "SELECT NRILDIM.*, NSCHEDIM.DESCRIZIONE ",
        "FROM STAAMPDB.NRILDIM NRILDIM ",
        "LEFT JOIN STAAMPDB.NSCHEDIM NSCHEDIM "
        "ON NRILDIM.NUMERO_RIFERIMENTO = NSCHEDIM.NUMERO_RIFERIMENTO ",
        "WHERE 1=1",
    ]
    params = []

    if articolo:
        parts.append("AND NRILDIM.ARTICOLO LIKE ?")
        params.append(f"{articolo}%")
    if numero_riferimento:
        parts.append("AND NRILDIM.NUMERO_RIFERIMENTO = ?")
        params.append(numero_riferimento)
    if date_from:
        parts.append("AND NRILDIM.DATA_RILEVAMENTO >= ?")
        params.append(date_from.replace('-', ''))
    if date_to:
        parts.append("AND NRILDIM.DATA_RILEVAMENTO <= ?")
        params.append(date_to.replace('-', ''))
    if not (articolo or numero_riferimento or date_from or date_to):
        parts.append("AND NRILDIM.DATA_RILEVAMENTO LIKE '2025%'")

    return " ".join(parts), tuple(params)


def _format_nrildim_df(df):
    """Apply date/time/MIS formatting to a raw NRILDIM dataframe."""
    import pandas as pd

    if 'DATA_RILEVAMENTO' in df.columns:
        df['DATA_RILEVAMENTO'] = df['DATA_RILEVAMENTO'].astype(str).str.strip()
        mask = df['DATA_RILEVAMENTO'].str.len() == 8
        df.loc[mask, 'DATA_RILEVAMENTO'] = (
            df.loc[mask, 'DATA_RILEVAMENTO'].str[:4] + '-' +
            df.loc[mask, 'DATA_RILEVAMENTO'].str[4:6] + '-' +
            df.loc[mask, 'DATA_RILEVAMENTO'].str[6:8]
        )

    if 'ORA_RILEVAMENTO' in df.columns:
        df['ORA_RILEVAMENTO'] = df['ORA_RILEVAMENTO'].astype(str).str.strip()
        mask_t = df['ORA_RILEVAMENTO'].str.len() == 6
        df.loc[mask_t, 'ORA_RILEVAMENTO'] = (
            df.loc[mask_t, 'ORA_RILEVAMENTO'].str[:2] + ':' +
            df.loc[mask_t, 'ORA_RILEVAMENTO'].str[2:4] + ':' +
            df.loc[mask_t, 'ORA_RILEVAMENTO'].str[4:6]
        )

    if 'NUMERO_STAMPATA' in df.columns:
        df['NUMERO_STAMPATA'] = df['NUMERO_STAMPATA'].astype(str).str.strip().str[-1:]
    if 'NUMERO_FIGURA' in df.columns:
        df['NUMERO_FIGURA'] = df['NUMERO_FIGURA'].astype(str).str.strip().str[-1:]

    mis_present = [c for c in _RESULTS_COLUMNS if c in df.columns]
    if mis_present:
        df[mis_present] = df[mis_present].apply(pd.to_numeric, errors='coerce')
        df[mis_present] = df[mis_present] / 10000.0

    return df


@placeholder_bp.route('/kontrola-jakosci-lab')
@module_required('zarzadzanie')
def kontrola_jakosci_lab():
    """Wyniki pomiarów laboratoryjnych z MOSYS NRILDIM."""
    import pandas as pd
    from MOSYS_data_functions import get_pervasive

    articolo          = request.args.get('articolo', '').strip()
    date_from         = request.args.get('date_from', '').strip()
    date_to           = request.args.get('date_to', '').strip()
    numero_riferimento = request.args.get('numero_riferimento', '').strip()
    action            = request.args.get('action', '').strip()

    riferimento_options = []
    if action == 'fetch':
        try:
            nschedim_df = get_pervasive(
                "SELECT NUMERO_RIFERIMENTO, DESCRIZIONE, FLAG_RIMOSSO FROM STAAMPDB.NSCHEDIM"
            )
            riferimento_options = nschedim_df.to_dict('records')
        except Exception as e:
            current_app.logger.warning(f"kontrola_jakosci_lab: NSCHEDIM fetch failed: {e}")

    if action != 'fetch':
        return render_template(
            'placeholder/kontrola_jakosci_lab.html',
            columns=_TABLE_COLUMNS,
            data=None,
            riferimento_options=[],
            column_labels=KOLUMN_ETYKIETY,
            articolo=articolo,
            date_from=date_from,
            date_to=date_to,
            numero_riferimento=numero_riferimento,
        )

    query, params = _build_nrildim_query(articolo, numero_riferimento, date_from, date_to)
    try:
        df = get_pervasive(query, params=params)
    except Exception as e:
        current_app.logger.error(f"kontrola_jakosci_lab DB error: {e}", exc_info=True)
        df = pd.DataFrame()

    df = _format_nrildim_df(df)

    valid_mis = [c for c in _RESULTS_COLUMNS if c in df.columns and df[c].notna().any()]

    if riferimento_options and 'NUMERO_RIFERIMENTO' in df.columns:
        unique_refs = set(df['NUMERO_RIFERIMENTO'].dropna().unique())
        riferimento_options = [o for o in riferimento_options if o['NUMERO_RIFERIMENTO'] in unique_refs]

    final_columns = [c for c in _TABLE_COLUMNS if c not in _RESULTS_COLUMNS] + valid_mis
    final_columns = [c for c in final_columns if c in df.columns]

    data = df[final_columns].to_dict(orient='records') if not df.empty else []

    return render_template(
        'placeholder/kontrola_jakosci_lab.html',
        columns=final_columns,
        data=data,
        riferimento_options=riferimento_options,
        column_labels=KOLUMN_ETYKIETY,
        articolo=articolo,
        date_from=date_from,
        date_to=date_to,
        numero_riferimento=numero_riferimento,
    )


@placeholder_bp.route('/kontrola-jakosci-lab/graph')
@module_required('zarzadzanie')
def kontrola_jakosci_lab_graph():
    """Wykres trend pomiarów z MOSYS – Cp/Cpk."""
    import json
    import pandas as pd
    from MOSYS_data_functions import get_pervasive

    articolo           = request.args.get('articolo', '').strip()
    numero_riferimento = request.args.get('numero_riferimento', '').strip()
    date_from          = request.args.get('date_from', '').strip()
    date_to            = request.args.get('date_to', '').strip()

    parts = [
        "SELECT NRILDIM.*, NSCHEDIM.DESCRIZIONE, NSCHEDIM.VALORE_NOMINALE ",
        "FROM STAAMPDB.NRILDIM NRILDIM ",
        "LEFT JOIN STAAMPDB.NSCHEDIM NSCHEDIM "
        "ON NRILDIM.NUMERO_RIFERIMENTO = NSCHEDIM.NUMERO_RIFERIMENTO ",
        "WHERE 1=1",
    ]
    params = []
    if articolo:
        parts.append("AND NRILDIM.ARTICOLO LIKE ?")
        params.append(f"{articolo}%")
    if numero_riferimento:
        parts.append("AND NRILDIM.NUMERO_RIFERIMENTO = ?")
        params.append(numero_riferimento)
    if date_from:
        parts.append("AND NRILDIM.DATA_RILEVAMENTO >= ?")
        params.append(date_from.replace('-', ''))
    if date_to:
        parts.append("AND NRILDIM.DATA_RILEVAMENTO <= ?")
        params.append(date_to.replace('-', ''))
    parts.append("ORDER BY NRILDIM.DATA_RILEVAMENTO, NRILDIM.ORA_RILEVAMENTO")
    query = " ".join(parts)

    try:
        df = get_pervasive(query, params=tuple(params))
    except Exception as e:
        current_app.logger.error(f"kontrola_jakosci_lab_graph DB error: {e}", exc_info=True)
        return render_template('placeholder/kontrola_jakosci_lab_graph.html',
                               error="Błąd pobierania danych z bazy MOSYS.")

    if df.empty:
        return render_template('placeholder/kontrola_jakosci_lab_graph.html',
                               error="Brak danych dla wybranych filtrów.")

    mis_present = [c for c in _RESULTS_COLUMNS if c in df.columns]
    if mis_present:
        df[mis_present] = df[mis_present].apply(pd.to_numeric, errors='coerce')
        df[mis_present] = df[mis_present] / 10000.0

    df['MIS_AVG'] = df[mis_present].mean(axis=1, skipna=True)

    df['DATA_RILEVAMENTO'] = df['DATA_RILEVAMENTO'].astype(str).str.strip()
    df['ORA_RILEVAMENTO']  = df['ORA_RILEVAMENTO'].astype(str).str.strip().str.zfill(6)
    df['DATETIME'] = (
        df['DATA_RILEVAMENTO'] + ' ' +
        df['ORA_RILEVAMENTO'].str[:2] + ':' +
        df['ORA_RILEVAMENTO'].str[2:4] + ':' +
        df['ORA_RILEVAMENTO'].str[4:6]
    )

    numero_figura_values = df['NUMERO_FIGURA'].dropna().unique().tolist()

    valore_nominale = usl = lsl = None
    if numero_riferimento:
        try:
            tol_df = get_pervasive(
                """SELECT CODICE_ARTICOLO, RIF_MISURA, UN_MIS, VALORE_NOMINALE,
                          SEGNO_TOLL_INF, TOLL_INF, SEGNO_TOLL_SUP, TOLL_SUP
                   FROM STAAMPDB.SCHEDIM1 SCHEDIM1
                   WHERE SCHEDIM1.RIF_MISURA = ?""",
                params=(numero_riferimento,),
            )
            if not tol_df.empty:
                r = tol_df.iloc[0]
                valore_nominale = float(r['VALORE_NOMINALE']) if pd.notna(r['VALORE_NOMINALE']) else None
                sign_inf = str(r['SEGNO_TOLL_INF']).strip() if pd.notna(r['SEGNO_TOLL_INF']) else '+'
                sign_sup = str(r['SEGNO_TOLL_SUP']).strip() if pd.notna(r['SEGNO_TOLL_SUP']) else '+'
                toll_inf = float(r['TOLL_INF']) if pd.notna(r['TOLL_INF']) else 0.0
                toll_sup = float(r['TOLL_SUP']) if pd.notna(r['TOLL_SUP']) else 0.0
                if valore_nominale is not None:
                    lim_inf = valore_nominale + toll_inf if sign_inf == '+' else valore_nominale - toll_inf
                    lim_sup = valore_nominale + toll_sup if sign_sup == '+' else valore_nominale - toll_sup
                    usl = max(lim_inf, lim_sup)
                    lsl = min(lim_inf, lim_sup)
        except Exception as e:
            current_app.logger.warning(f"kontrola_jakosci_lab_graph tolerance fetch failed: {e}")

    capability_data = {}
    if usl is not None and lsl is not None:
        for figura in numero_figura_values:
            vals = df[df['NUMERO_FIGURA'] == figura]['MIS_AVG'].dropna()
            if len(vals) > 1:
                mean = vals.mean()
                std  = vals.std()
                if std > 0:
                    cp   = (usl - lsl) / (6 * std)
                    cpk  = min((usl - mean) / (3 * std), (mean - lsl) / (3 * std))
                    capability_data[str(figura)] = {
                        'cp': round(cp, 3), 'cpk': round(cpk, 3),
                        'mean': round(mean, 3), 'std': round(std, 3),
                    }

    chart_data = {}
    for figura in numero_figura_values:
        sub = df[df['NUMERO_FIGURA'] == figura].dropna(subset=['MIS_AVG'])
        chart_data[str(figura)] = {
            'labels': sub['DATETIME'].tolist(),
            'values': sub['MIS_AVG'].round(3).tolist(),
        }

    descrizione = df['DESCRIZIONE'].iloc[0] if 'DESCRIZIONE' in df.columns else numero_riferimento

    return render_template(
        'placeholder/kontrola_jakosci_lab_graph.html',
        chart_data=json.dumps(chart_data),
        descrizione=descrizione,
        numero_figura_count=len(numero_figura_values),
        valore_nominale=valore_nominale,
        usl=usl,
        lsl=lsl,
        capability_data=json.dumps(capability_data),
        column_labels=KOLUMN_ETYKIETY,
        articolo=articolo,
        date_from=date_from,
        date_to=date_to,
    )

@placeholder_bp.route('/admin/backfill-opis', methods=['POST'])
@module_required('admin')
def admin_backfill_opis():
    """One-time backfill: fetch opis_niezgodnosci from MOSYS for all records where it is NULL/empty."""
    try:
        from MOSYS_data_functions import get_batch_niezgodnosc_details

        records = DaneRaportu.query.filter(
            DaneRaportu.nr_niezgodnosci.isnot(None),
            DaneRaportu.nr_niezgodnosci != '',
            db.or_(
                DaneRaportu.opis_niezgodnosci.is_(None),
                DaneRaportu.opis_niezgodnosci == ''
            )
        ).all()

        total = len(records)
        updated = 0
        skipped = 0
        BATCH = 50

        for i in range(0, total, BATCH):
            batch = records[i:i + BATCH]
            nr_list = [r.nr_niezgodnosci for r in batch]
            mosys_data = get_batch_niezgodnosc_details(nr_list)

            for report in batch:
                detail = mosys_data.get(report.nr_niezgodnosci)
                if detail and detail.get('opis_niezgodnosci'):
                    report.opis_niezgodnosci = detail['opis_niezgodnosci']
                    updated += 1
                else:
                    skipped += 1

            db.session.commit()

        return jsonify({
            'success': True,
            'total_candidates': total,
            'updated': updated,
            'skipped': skipped,
        })
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Backfill opis_niezgodnosci failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@placeholder_bp.route('/admin/sync-excel', methods=['GET', 'POST'])
@module_required('admin')
def admin_sync_excel():
    """Admin endpoint to manually force Excel data sync."""
    try:
        from app.utils.excel_sync import force_sync
        sync_result = force_sync()
        return jsonify({
            'success': True,
            'sync_result': sync_result
        })
    except Exception as e:
        current_app.logger.error(f"Manual Excel sync failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
