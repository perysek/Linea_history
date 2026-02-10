# LINEA System - Feature Implementation TODO

## 1. Wykaz zablokowanych detali (Blocked Parts List)
**Route:** `/wykaz-zablokowanych`
**Blueprint:** `placeholder.wykaz_zablokowanych`

### Tasks:
- [ ] 🔍 Pobranie danych o zablokowanych detalach z MOSYS
- [ ] 📊 Tabela z listą wszystkich zablokowanych detali
- [ ] 🔎 Filtrowanie po: kod detalu, NR NC, zamówienie, data
- [ ] 📈 Sumowanie ilości zablokowanych detali
- [ ] 📅 Wyświetlanie czasu blokady (data rozpoczęcia - dzisiaj)
- [ ] ⚡ Eksport danych do Excel
- [ ] 🔗 Link do szczegółów NC z modalu LINEA

### Technical Notes:
- Use `get_blocked_parts_qty()` function from `MOSYS_data_functions.py`
- Query MOSYS tables: NOTCOJAN, COLLAUDO
- Integrate with existing modal functionality from LINEA

---

## 2. LINEA Uwagi (Production Notes)
**Route:** `/linea/`
**Blueprint:** `linea.index`
**Status:** ✅ IMPLEMENTED

### Completed Features:
- ✅ Display production notes from NOTCOJAN table
- ✅ Filter by date range, COMMESSA, NC number, machine, etc.
- ✅ Modal with repair details and blocked parts quantities
- ✅ Column-based search and sorting
- ✅ TYP_UWAGI value mapping to Polish labels
- ✅ Yellow indicator for closed NCs without actions

---

## 3. Dane z selekcji (Selection Data)
**Route:** `/dane-selekcji`
**Blueprint:** `placeholder.dane_selekcji`

### Tasks:
- [ ] 🔍 Odczyt danych z tabeli SELEKCJA w MOSYS
- [ ] 📊 Tabela z rekordami selekcji produkcji
- [ ] 🔎 Filtrowanie po: zamówienie, maszyna, operator, data
- [ ] 📈 Wyświetlanie ilości wyprodukowanych detali
- [ ] ⏱️ Czasy produkcji i przerwy
- [ ] 📉 Statystyki efektywności produkcji
- [ ] ⚡ Eksport do Excel

### Technical Notes:
- Query MOSYS SELEKCJA table
- Calculate production time = end_time - start_time
- Show efficiency metrics (OEE, downtime, etc.)

---

## 4. Analiza danych (Data Analysis)
**Route:** `/analiza-danych`
**Blueprint:** `placeholder.analiza_danych`

### Tasks:
- [ ] 📊 Dashboard z głównymi wskaźnikami KPI
- [ ] 📈 Wykresy trendów niezgodności (dzienny, tygodniowy, miesięczny)
- [ ] 🎯 Analiza Pareto - najczęstsze typy niezgodności
- [ ] 🏭 Statystyki per maszyna / linia produkcyjna
- [ ] 📉 Wskaźniki jakości (PPM, FTY, scrap rate)
- [ ] ⏱️ Średni czas naprawy (MTTR)
- [ ] 📅 Porównania okres do okresu
- [ ] 💾 Zapis raportów do PDF

### Technical Notes:
- Use Chart.js or similar for visualizations
- Aggregate data from NOTCOJAN, RIPARAZ, COLLAUDO
- Implement date range comparison functionality
- Generate PDF reports using ReportLab or WeasyPrint

### KPI Formulas:
- **PPM** (Parts Per Million defects) = (Defects / Total Parts) × 1,000,000
- **FTY** (First Time Yield) = (Good Parts / Total Parts) × 100%
- **MTTR** (Mean Time To Repair) = Total Repair Time / Number of Repairs

---

## 5. Dane zamówień produkcyjnych (Production Orders Data)
**Route:** `/dane-zamowien`
**Blueprint:** `placeholder.dane_zamowien`

### Tasks:
- [ ] 🔍 Pobranie danych o zamówieniach z MOSYS (COLLAUDO)
- [ ] 📊 Lista wszystkich aktywnych zamówień
- [ ] 🔎 Filtrowanie po: COMMESSA, kod detalu, forma, maszyna
- [ ] 📈 Status zamówienia (w produkcji, zakończone, zatrzymane)
- [ ] 📅 Planowana data zakończenia vs. rzeczywista
- [ ] 🎯 Postęp realizacji (ilość zrobiona / planowana)
- [ ] 🔗 Powiązane NC i naprawy
- [ ] ⚡ Eksport do Excel

### Technical Notes:
- Query COLLAUDO table for order information
- Join with NOTCOJAN to show related NCs
- Calculate progress percentage
- Color-code orders by status (green=completed, yellow=in progress, red=delayed)

---

## 6. Utrzymanie form (Mold Maintenance)
**Route:** `/utrzymanie-form`
**Blueprint:** `placeholder.utrzymanie_form`

### Tasks:
- [ ] 🔍 Rejestr form wtryskowych z MOSYS (STAMPO)
- [ ] 📊 Lista form z statusem i lokalizacją
- [ ] 🔧 Historia napraw każdej formy
- [ ] 📈 Licznik cykli produkcyjnych
- [ ] ⏰ Harmonogram przeglądów prewencyjnych
- [ ] ⚠️ Alerty o przekroczeniu limitów cykli
- [ ] 💰 Koszty napraw i utrzymania
- [ ] 📄 Dokumentacja techniczna form

### Technical Notes:
- Query STAMPO table for mold registry
- Join with RIPARAZ for repair history
- Implement alert system for cycle limits
- Create preventive maintenance schedule based on cycle count
- Track maintenance costs per mold

---

## 7. Kontrola jakości (Quality Control)
**Route:** `/kontrola-jakosci`
**Blueprint:** `placeholder.kontrola_jakosci`

### Tasks:
- [ ] 🔍 Wyniki kontroli z tabeli COLLAUDO
- [ ] 📊 Rejestr kontroli pierwszych detali (FAI)
- [ ] ✅ Status kontroli (zaakceptowana, odrzucona, w toku)
- [ ] 📈 Wykresy defektów per detal/forma
- [ ] 👤 Lista kontrolerów i ich aktywność
- [ ] ⏱️ Czasy kontroli i zatwierdzenia
- [ ] 📋 Plany kontroli per produkt
- [ ] 📸 Załączniki zdjęć defektów

### Technical Notes:
- Query COLLAUDO for inspection results
- Implement FAI (First Article Inspection) tracking
- Create defect charts grouped by part/mold
- Track inspector performance metrics
- File upload functionality for defect photos
- Link to inspection plans/drawings

---

## Common Technical Requirements

### Database Access:
- All features use pyodbc connection via `app/database.py`
- Read-only access to MOSYS database
- Connection parameters in `config.py`

### UI/UX Standards:
- Follow Refined Minimal Design System from `app/static/css/linea.css`
- Use Heroicons for all icons
- Implement client-side filtering and sorting
- Add loading states for async operations
- Include empty states with helpful messages

### Export Functionality:
- Use `openpyxl` for Excel exports
- Include all visible columns + applied filters in export
- Add timestamp to exported filenames

### Security:
- Input validation for all user inputs
- SQL injection prevention (use parameterized queries)
- XSS prevention (escape HTML output)

---

## Development Priorities

### Phase 1 (High Priority):
1. Wykaz zablokowanych detali - most requested by users
2. Analiza danych - management reporting needs

### Phase 2 (Medium Priority):
3. Dane zamówień produkcyjnych - production planning
4. Kontrola jakości - quality assurance

### Phase 3 (Lower Priority):
5. Dane z selekcji - detailed production analysis
6. Utrzymanie form - maintenance planning

---

## Notes:
- All placeholder pages are accessible at their respective routes
- Each page displays "W trakcie tworzenia" (Under construction) message
- TODO lists are visible to users on placeholder pages
- Active development should follow the priority order above
