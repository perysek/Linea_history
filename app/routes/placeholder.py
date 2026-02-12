"""Placeholder routes for features under development."""
from flask import Blueprint, render_template, jsonify, request, current_app
from sqlalchemy.orm import joinedload
from sqlalchemy import func
from datetime import datetime, timedelta
from app import db
from app.models.sorting_area import DaneRaportu, BrakiDefektyRaportu, Operator, KategoriaZrodlaDanych
from app.utils.excel_sync import sync_new_excel_data

placeholder_bp = Blueprint('placeholder', __name__)


@placeholder_bp.route('/wykaz-zablokowanych')
def wykaz_zablokowanych():
    """Lista zablokowanych detali z MOSYS."""
    try:
        from MOSYS_data_functions import get_all_blocked_parts
        parts = get_all_blocked_parts()

        # Format dates for display
        for part in parts:
            if part.get('data_niezgodnosci'):
                part['data_niezgodnosci'] = part['data_niezgodnosci'].strftime('%Y-%m-%d')

            # Format production date range
            min_date = part.get('data_produkcji_min')
            max_date = part.get('data_produkcji_max')

            if min_date and max_date:
                if min_date == max_date:
                    part['produced'] = min_date.strftime('%Y/%m/%d')
                else:
                    part['produced'] = f"{min_date.strftime('%Y/%m/%d')} - {max_date.strftime('%Y/%m/%d')}"
            else:
                part['produced'] = '-'

        total_blocked = sum(p['ilosc_zablokowanych'] for p in parts)
        return render_template('placeholder/wykaz_zablokowanych.html', parts=parts, total_blocked=total_blocked)
    except Exception as e:
        print(f"Error fetching blocked parts: {e}")
        # Return None to trigger MOSYS connection error display
        return render_template('placeholder/wykaz_zablokowanych.html', parts=None, total_blocked=0)


@placeholder_bp.route('/wykaz-zablokowanych/boxes/<nr_niezgodnosci>')
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
    if filters['nr_instrukcji']:
        query = query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        query = query.join(BrakiDefektyRaportu).filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    # Apply sorting
    valid_sort_columns = ['data_selekcji', 'nr_raportu', 'nr_niezgodnosci', 'data_niezgodnosci',
                          'nr_zamowienia', 'kod_detalu', 'nr_instrukcji', 'selekcja_na_biezaco',
                          'ilosc_detali_sprawdzonych', 'czas_pracy', 'zalecana_wydajnosc']
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

    # Lazy load missing MOSYS data
    reports_needing_mosys = [r for r in reports
                             if r.data_niezgodnosci is None and r.nr_niezgodnosci]

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
    if filters['nr_instrukcji']:
        query = query.filter(DaneRaportu.nr_instrukcji.ilike(f"%{filters['nr_instrukcji']}%"))
    if filters['defekt']:
        query = query.join(BrakiDefektyRaportu).filter(
            BrakiDefektyRaportu.defekt.ilike(f"%{filters['defekt']}%")
        )

    # Apply sorting
    valid_sort_columns = ['data_selekcji', 'nr_raportu', 'nr_niezgodnosci', 'data_niezgodnosci',
                          'nr_zamowienia', 'kod_detalu', 'nr_instrukcji', 'selekcja_na_biezaco',
                          'ilosc_detali_sprawdzonych', 'czas_pracy', 'zalecana_wydajnosc']
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


@placeholder_bp.route('/analiza-danych')
def analiza_danych():
    """Analiza danych - under construction."""
    return render_template('placeholder/analiza_danych.html')


@placeholder_bp.route('/dane-zamowien')
def dane_zamowien():
    """Dane zamówień produkcyjnych - under construction."""
    return render_template('placeholder/dane_zamowien.html')


@placeholder_bp.route('/utrzymanie-form')
def utrzymanie_form():
    """Utrzymanie form - under construction."""
    return render_template('placeholder/utrzymanie_form.html')


@placeholder_bp.route('/kontrola-jakosci')
def kontrola_jakosci():
    """Kontrola jakości - under construction."""
    return render_template('placeholder/kontrola_jakosci.html')


@placeholder_bp.route('/admin/sync-excel', methods=['POST'])
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
