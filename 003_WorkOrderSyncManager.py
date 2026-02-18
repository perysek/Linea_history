"""
WorkOrder Synchronization Manager - Versione 19.1 (Polars + Discrepancy Tracking)

Gestisce la sincronizzazione dei dati delle commesse (Work Orders) tra il sistema
legacy Pervasive e MySQL con tracking dettagliato delle discrepanze.

---
Versione 19.2:
- Fix: workOrder nel file diff ora mostra il valore completo (non troncato)
- Fix: Aggiunto tracking date woStart e woEnd nel file diff tra parentesi quadre
- Fix: Aggiunto controllo per date invertite (woEnd < woStart)
- Migliorato logging delle discrepanze con informazioni temporali complete
"""

# Importazioni della libreria standard
import logging
import sys
import json
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Any
from collections import defaultdict
from pathlib import Path

# Importazioni di terze librerie correlate
import polars as pl
from sqlalchemy import text, MetaData, Table
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
#from pyinstrument import Profiler

# Importazioni di app locali
from constants import PLANT
from BaseSyncManager import BaseSyncManager
from DatabaseManager import db_manager
from Decorators import timer
from Functions import functions

# Configurazione del logger a livello di modulo
logger = logging.getLogger(__name__)

class DiscrepancyTracker:
    """
    Classe per tracciare le discrepanze durante la sincronizzazione.
    Mantiene i dati in memoria e li scrive su file solo alla fine.
    """
    
    def __init__(self, plant: str):
        self.plant = plant
        self.start_time = datetime.now()
        self.discrepancies: Dict[str, int] = defaultdict(int)
        self.output_file = Path("./Notes/diff_workOrder.txt")
    
    def add_discrepancy(self, discrepancy_type: str, workorder: str, press: str = None, 
                       mold: str = None, article: str = None, customer: str = None, 
                       reason: str = None, additional_info: str = None,
                       woStart: str = None, woEnd: str = None):
        """
        Aggiunge una discrepanza al tracker.
        
        Args:
            discrepancy_type: Tipo di discrepanza (PRESS_NOT_FOUND, MOLD_NOT_FOUND, etc.)
            workorder: Codice work order COMPLETO (non troncato)
            press, mold, article, customer: Codici correlati
            reason: Descrizione del problema
            additional_info: Informazioni aggiuntive
            woStart, woEnd: Date di inizio e fine work order (formato stringa)
        """
        # Costruisci la chiave univoca per raggruppare discrepanze identiche
        parts = []
        
        # Formatta workOrder con date se disponibili
        wo_display = f"workOrder={workorder}"
        if woStart or woEnd:
            date_info = []
            if woStart:
                date_info.append(f"start={woStart}")
            if woEnd:
                date_info.append(f"end={woEnd}")
            wo_display += f" [{', '.join(date_info)}]"
        parts.append(wo_display)
        
        if press:
            parts.append(f"press={press}")
        if mold:
            parts.append(f"mold={mold}")
        if article:
            parts.append(f"article={article}")
        if customer:
            parts.append(f"customer={customer}")
        if reason:
            parts.append(f"reason={reason}")
        if additional_info:
            parts.append(f"info={additional_info}")
        
        key = f"{discrepancy_type} | {' | '.join(parts)}"
        self.discrepancies[key] += 1
    
    def write_to_file(self):
        """
        Scrive tutte le discrepanze accumulate nel file, in append mode.
        """
        if not self.discrepancies:
            logger.info("Nessuna discrepanza da registrare.")
            return
        
        total_count = sum(self.discrepancies.values())
        unique_count = len(self.discrepancies)
        
        try:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                # Header della sessione
                f.write("=" * 150 + "\n")
                f.write(f"SESSION START: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')} | PLANT: {self.plant}\n")
                f.write(f"Total discrepancies: {total_count} (Unique: {unique_count})\n")
                f.write("=" * 150 + "\n")
                
                # Ordina le discrepanze per tipo e poi per conteggio (decrescente)
                sorted_discrepancies = sorted(
                    self.discrepancies.items(), 
                    key=lambda x: (x[0].split(' | ')[0], -x[1])
                )
                
                # Scrivi ogni discrepanza
                for disc_key, count in sorted_discrepancies:
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    count_suffix = f" [x{count}]" if count > 1 else ""
                    f.write(f"[{timestamp}] {disc_key}{count_suffix}\n")
                
                f.write("\n")
            
            logger.info(f"Discrepanze scritte su {self.output_file}: {total_count} totali ({unique_count} uniche)")
            
        except Exception as e:
            logger.error(f"Errore durante la scrittura del file di discrepanze: {e}")




class WorkOrderSyncManager(BaseSyncManager):
    """
    Gestisce la sincronizzazione dei dati delle commesse su `woStatic_tbl` e `woDynamic_tbl`.
    """

    def __init__(self, plant: str):
        super().__init__(plant, "woStatic_tbl")
        self._presses_cache: Dict[str, int] = {}
        self._molds_cache: Dict[str, int] = {}
        self._articles_cache: Dict[str, int] = {}
        self._customers_cache: Dict[str, int] = {}
        self.date_range: Tuple[datetime, datetime] = self._get_date_range()
        self.discrepancy_tracker = DiscrepancyTracker(plant)

    def synchronize(self) -> Tuple[int, int, int]:
        start_dt, end_dt = self.date_range
        logger.info("=" * 80)
        logger.info(f"AVVIO SINCRONIZZAZIONE WORKORDER per plant '{self.plant}'")
        print(f"Intervallo di date calcolato: DA {start_dt.strftime('%Y-%m-%d')} A {end_dt.strftime('%Y-%m-%d')}")
        logger.info("=" * 80)

        try:
            with db_manager.get_sessions() as (mosys_session, mysql_session):
                
                logger.info("FASE 1: Identificazione Inserimenti e Aggiornamenti.")
                legacy_data = self.fetch_all_legacy_data(mosys_session)
                
                inserted_count, updated_count, deleted_count = 0, 0, 0
                
                if not legacy_data.is_empty():
                    legacy_codes = legacy_data['workOrder'].unique().to_list()
                    current_data_aligned = self._fetch_current_data_by_codes(mysql_session, legacy_codes)
                    
                    if current_data_aligned.is_empty():
                        logger.info("Nessun dato corrente corrispondente. Tutti i dati legacy saranno inseriti.")
                        to_insert = legacy_data
                        to_update = pl.DataFrame()
                    else:
                        merged_data = self.compare_datasets(legacy_data, current_data_aligned)
                        to_insert, to_update, _ = self.identify_changes(merged_data)
                    
                    inserted_count, updated_count = self._execute_multi_table_operations_on_changes(to_insert, to_update, mysql_session)
                else:
                    logger.warning("Nessun dato attivo trovato in Pervasive nell'intervallo.")

                logger.info("FASE 2: Identificazione Cancellazioni.")
                current_data_in_window = self.fetch_current_data(mysql_session)
                
                if not current_data_in_window.is_empty():
                    legacy_codes = set(legacy_data['workOrder'].to_list()) if not legacy_data.is_empty() else set()
                    current_codes = set(current_data_in_window['workOrder'].to_list())
                    deleted_codes = list(current_codes - legacy_codes)
                    
                    if deleted_codes:
                        to_delete_verified = current_data_in_window.filter(pl.col("workOrder").is_in(deleted_codes))
                        deleted_count = self._execute_multi_table_deletes(to_delete_verified, mysql_session)
                else:
                    logger.info("Nessun dato corrente nella finestra temporale. Nessuna cancellazione necessaria.")

                # Scrivi le discrepanze accumulate su file
                self.discrepancy_tracker.write_to_file()

                logger.info(f"Sincronizzazione completata: {inserted_count} inseriti, {updated_count} aggiornati, {deleted_count} eliminati.")
                return inserted_count, updated_count, deleted_count

        except Exception as e:
            logger.error(f"Errore critico durante la sincronizzazione WorkOrder: {e}", exc_info=True)
            # Scrivi comunque le discrepanze anche in caso di errore
            self.discrepancy_tracker.write_to_file()
            raise

    def get_legacy_column_types(self, table_name: str) -> Dict[str, pl.DataType]:
        """
        Forza i tipi di colonne specifici per le tabelle legacy.
        """
        if table_name == "DATITURN (raw)":
            return {
                'workOrder': pl.String,
                'PEZZI_BUONI': pl.Int64,
                'PZ_SCARTO_AVVIO': pl.Int64,
                'PZ_SCARTO_AUTOM': pl.Int64,
                'PEZZI_SCARTO': pl.Int64,
                'SCARTI_MAN': pl.Int64,
                'PEZZI_SEGR': pl.Int64,
                'PEZZI_SEGR_SCARTO': pl.Int64,
                'PEZZI_CONF': pl.Int64,
                'pieceTime': pl.String,
            }
        elif table_name == "PLANNING/FINPLAN":
            return {
                'workOrder': pl.String,
                'press': pl.String,
                'mold': pl.String,
                'article': pl.String,
                'state': pl.String,
                'woTest': pl.String,
                'woStart_date': pl.String,
                'woStart_time': pl.String,
                'woEnd_date': pl.String,
                'woEnd_time': pl.String,
            }
        elif table_name == "JOBSETUP/OFFARB":
            return {
                'workOrder': pl.String,
                'program': pl.String,
                'progRegDate': pl.String,
            }
        elif table_name == "SEGREGA":
            return {
                'workOrder': pl.String,
                'totalSegrPz': pl.Int64,
                'scrapedSegrPz': pl.Int64,
            }
        elif table_name == "MAGCONF":
            return {
                'workOrder': pl.String,
                'packedPz': pl.Int64,
            }
        else:
            return {}

    def _get_datiturn_schema(self) -> Dict[str, pl.DataType]:
        return {
            'workOrder': pl.String,
            'PEZZI_BUONI': pl.Int64,
            'PZ_SCARTO_AVVIO': pl.Int64,
            'PZ_SCARTO_AUTOM': pl.Int64,
            'PEZZI_SCARTO': pl.Int64,
            'SCARTI_MAN': pl.Int64,
            'PEZZI_SEGR': pl.Int64,
            'PEZZI_SEGR_SCARTO': pl.Int64,
            'PEZZI_CONF': pl.Int64,
            'pieceTime': pl.String,
        }

    def _get_jobsetup_schema(self) -> Dict[str, pl.DataType]:
        return {
            'workOrder': pl.String,
            'program': pl.String,
            'progRegDate': pl.String,
        }
    
    def _fetch_legacy_table_with_schema(self, session: Session, query: str, table_name: str) -> pl.DataFrame:
        try:
            # Recuperiamo i tipi forzati PRIMA di leggere il DB
            overrides = self.get_legacy_column_types(table_name)
            
            # Caricamento con schema_overrides per prevenire l'errore sui Decimal
            df = pl.read_database(
                query=query, 
                connection=session.connection(),
                schema_overrides=overrides if overrides else None
            )
            return df
        except Exception as e:
            logger.error(f"Errore nel recupero dati da '{table_name}': {e}")
            raise

    def get_legacy_query(self) -> str:
        pass

    def transform_legacy_data(self, df: pl.DataFrame) -> pl.DataFrame:
        pass

    def get_primary_key_columns(self) -> List[str]:
        return ['workOrder', 'plant']
    
    def get_comparison_columns(self) -> List[str]:
        static_cols = ['idPress', 'idMold', 'idArticle', 'idCustomer', 'woStart', 'woEnd', 'woTest', 'figures', 'woTotPieces', 'cycle']
        dynamic_cols = ['program', 'progRegDate', 'woState', 'usedFigures', 'woCycle', 'woDonePieces', 'startScrapedPz', 'automScrapedPz', 'manualScrapedPz', 'totalSegrPz', 'scrapedSegrPz', 'packedPz']
        return static_cols + dynamic_cols

    def fetch_all_legacy_data(self, session: Session) -> pl.DataFrame:
        base_df = self._fetch_base_wo_data(session)
        if base_df.is_empty():
            return pl.DataFrame()

        work_orders_list = (
            base_df.get_column('workOrder')
                .str.strip_chars()
                .unique()
                .to_list()
        )

        datiturn_df = self._fetch_datiturn_data(session, work_orders_list)
        comlis_fincom_df = self._fetch_comlis_fincom_data(session, work_orders_list)
        jobsetup_df = self._fetch_jobsetup_data(session, work_orders_list)

        merged_df = base_df
        
        if not comlis_fincom_df.is_empty():
            merged_df = merged_df.join(comlis_fincom_df, on='workOrder', how='left')
        
        if not datiturn_df.is_empty():
            merged_df = merged_df.join(datiturn_df, on='workOrder', how='left')
            
            merged_df = merged_df.with_columns(
                pl.when(pl.col("woDonePieces").is_null())
                .then(pl.lit(0).cast(pl.Int64)) # <-- CORREZIONE: Imposta a 0 (zero)
                .otherwise(pl.col("woDonePieces"))
                .alias("woDonePieces_final")
            )
            merged_df = merged_df.drop('woDonePieces').rename({'woDonePieces_final': 'woDonePieces'})
        else:
            merged_df = merged_df.with_columns(
                pl.lit(0, dtype=pl.Int64).alias("woDonePieces")  # Usa 0 invece di woTotPieces
            )

        if not jobsetup_df.is_empty():
            merged_df = merged_df.join(jobsetup_df, on='workOrder', how='left')
        
        merged_df = merged_df.unique(subset=["workOrder"], keep="last")

        df = self._normalize_wo_data(merged_df)
        df = self._resolve_foreign_keys(df)
        df = self._validate_wo_data(df)

        final_cols = self.get_primary_key_columns() + self.get_comparison_columns()
        missing_cols = [c for c in final_cols if c not in df.columns]
        for col in missing_cols:
            df = df.with_columns(pl.lit(None).alias(col))

        df = df.select(final_cols)

        return df

    def fetch_current_data(self, mysql_session: Session) -> pl.DataFrame:
        start_date_obj, end_date_obj = self.date_range
        
        query = text("""
            SELECT s.*,
                d.program, d.progRegDate, d.woState, d.usedFigures, d.woCycle, d.woDonePieces,
                d.startScrapedPz, d.automScrapedPz, d.manualScrapedPz, d.totalSegrPz,
                d.scrapedSegrPz, d.packedPz
            FROM woStatic_tbl s
            LEFT JOIN woDynamic_tbl d ON s.idWorkOrder = d.idWorkOrder
            WHERE s.plant = :plant AND s.woStart <= :end_date AND (s.woEnd >= :start_date OR s.woEnd IS NULL)
        """)
        
        params = {"plant": self.plant, "start_date": start_date_obj, "end_date": end_date_obj}
        
        try:
            result = mysql_session.execute(query, params)
            rows = result.fetchall()
            if not rows:
                return pl.DataFrame()
            
            df = pl.DataFrame(
                rows, 
                schema=list(result.keys()),
                infer_schema_length=None
            )
            
            return self._clean_current_df(df)
        except Exception as e:
            logger.error(f"Errore nel recupero dati correnti: {e}")
            raise
    
    def _fetch_current_data_by_codes(self, mysql_session: Session, codes: List[str]) -> pl.DataFrame:
        if not codes: 
            return pl.DataFrame()
        
        query = text("""
            SELECT s.*,
                d.program, d.progRegDate, d.woState, d.usedFigures, d.woCycle, d.woDonePieces,
                d.startScrapedPz, d.automScrapedPz, d.manualScrapedPz, d.totalSegrPz,
                d.scrapedSegrPz, d.packedPz
            FROM woStatic_tbl s
            LEFT JOIN woDynamic_tbl d ON s.idWorkOrder = d.idWorkOrder
            WHERE s.plant = :plant AND s.workOrder IN :codes
        """)
        params = {"plant": self.plant, "codes": tuple(codes)}
        
        try:
            result = mysql_session.execute(query, params)
            rows = result.fetchall()
            if not rows:
                return pl.DataFrame()
            
            df = pl.DataFrame(
                rows, 
                schema=list(result.keys()),
                infer_schema_length=None
            )
            
            return self._clean_current_df(df)
        except Exception as e:
            logger.error(f"Errore nel recupero dati correnti per codice: {e}")
            raise

    def _fetch_base_wo_data(self, session: Session) -> pl.DataFrame:
        start_date_str = self.date_range[0].strftime('%Y%m%d')
        end_date_str = self.date_range[1].strftime('%Y%m%d')
        query = f"""
            SELECT COMMESSA AS workOrder,
                   PRESSA AS press,
                   STAMPO AS mold,
                   ARTICOLO AS article,                  
                   STATO AS state,
                   COMMESSA_PROVA AS woTest,
                   DATA_INIZIO_EFF AS woStart_date,
                   ORA_INIZIO_EFF AS woStart_time,
                   DATA_FINE_EFF AS woEnd_date,
                   ORA_FINE_EFF AS woEnd_time
            FROM PLANNING
            WHERE DATA_INIZIO_EFF <= '{end_date_str}' AND DATA_FINE_EFF >= '{start_date_str}'
            UNION ALL
            SELECT COMMESSA AS workOrder,
                   PRESSA AS press,
                   STAMPO AS mold,
                   ARTICOLO AS article,                   
                   STATO AS state,
                   COMMESSA_PROVA AS woTest,
                   DATA_INIZIO_EFF AS woStart_date,
                   ORA_INIZIO_EFF AS woStart_time,
                   DATA_FINE_EFF AS woEnd_date,
                   ORA_FINE_EFF AS woEnd_time
            FROM FINPLAN
            WHERE DATA_INIZIO_EFF <= '{end_date_str}' AND DATA_FINE_EFF >= '{start_date_str}'
        """
        df = self._fetch_legacy_table_with_schema(session, query, "PLANNING/FINPLAN")
        
        if not df.is_empty():
            return df.unique(subset=['workOrder'])
        return df

    def _fetch_datiturn_data_OLD(self, session: Session, work_orders_list: List[str]) -> pl.DataFrame:
        if not work_orders_list:
            return pl.DataFrame()
        
        wo_tuple = tuple(work_orders_list)
        segrega_fields = "0 as PEZZI_SEGR, 0 as PEZZI_SEGR_SCARTO," if self.plant == 'tn' else "PEZZI_SEGR, PEZZI_SEGR_SCARTO,"
        
        # 1. Query DATITURN: Scarichiamo le colonne RAW senza elaborazioni
        query = f"""
            SELECT COMMESSA AS workOrder, 
                PEZZI_BUONI, PZ_SCARTO_AVVIO, PZ_SCARTO_AUTOM, PEZZI_SCARTO,
                SCARTI_MAN, {segrega_fields} PEZZI_CONF,
                DATA_AGG_SYS, ORA_AGG_SYS
            FROM DATITURN WHERE COMMESSA IN {wo_tuple}
        """
        df_raw = self._fetch_legacy_table_with_schema(session, query, "DATITURN (raw)")

        STANDARD_COLUMNS = [
            'workOrder', 'firstPieceTime', 'lastPieceTime', 'woDonePieces', 
            'startScrapedPz', 'automScrapedPz', 'scrapedPz', 'manualScrapedPz', 
            'totalSegrPz', 'scrapedSegrPz', 'packedPz'
        ]
        
        # Inizializziamo i DataFrame per evitare errori di riferimento
        df_agg_pos = pl.DataFrame(schema={col: (pl.Utf8 if col in ['workOrder', 'firstPieceTime', 'lastPieceTime'] else pl.Int64) for col in STANDARD_COLUMNS})
        df_zero = pl.DataFrame()

        if not df_raw.is_empty():
            # Concatenazione rapida in Polars invece che in SQL
            df_raw = df_raw.with_columns(
                pl.when(pl.col("PEZZI_BUONI") > 0)
                .then(pl.col("DATA_AGG_SYS").cast(pl.Utf8) + pl.col("ORA_AGG_SYS").cast(pl.Utf8))
                .otherwise(None)
                .alias("pieceTime")
            ).rename({
                'PEZZI_SEGR': 'totalSegrPz', 'PEZZI_SEGR_SCARTO': 'scrapedSegrPz',
                'PZ_SCARTO_AVVIO': 'startScrapedPz', 'PZ_SCARTO_AUTOM': 'automScrapedPz',
                'PEZZI_SCARTO': 'scrapedPz', 'SCARTI_MAN': 'manualScrapedPz', 'PEZZI_CONF': 'packedPz'
            })
            
            df_pos = df_raw.filter(pl.col("PEZZI_BUONI") > 0)
            df_zero = df_raw.filter(pl.col("PEZZI_BUONI") == 0)

            if not df_pos.is_empty():
                df_agg_pos = df_pos.group_by('workOrder').agg(
                    pl.col('pieceTime').min().alias('firstPieceTime'),
                    pl.col('pieceTime').max().alias('lastPieceTime'),
                    pl.col('PEZZI_BUONI').sum().cast(pl.Int64).alias('woDonePieces'),
                    pl.col('startScrapedPz').sum().cast(pl.Int64),
                    pl.col('automScrapedPz').sum().cast(pl.Int64),
                    pl.col('scrapedPz').sum().cast(pl.Int64),
                    pl.col('manualScrapedPz').sum().cast(pl.Int64),
                    pl.col('totalSegrPz').sum().cast(pl.Int64),
                    pl.col('scrapedSegrPz').sum().cast(pl.Int64),
                    pl.col('packedPz').sum().cast(pl.Int64)
                )

        # 2. Gestione Work Orders mancanti o a zero
        wo_in_datiturn = set(df_raw.get_column('workOrder').unique().to_list()) if not df_raw.is_empty() else set()
        wo_missing = set(work_orders_list) - wo_in_datiturn
        wo_from_zero = set(df_zero.get_column('workOrder').to_list()) if not df_zero.is_empty() else set()
        
        wo_to_fetch_secondary = tuple(wo_missing | wo_from_zero)

        df_agg_zero = pl.DataFrame(schema=df_agg_pos.schema)
        if wo_to_fetch_secondary:
            # Anche qui scarichiamo le colonne RAW senza CONCAT
            query_secondary = f"""
                SELECT fc.COMMESSA AS workOrder,
                    fp.DATA_INIZIO_EFF, fp.ORA_INIZIO_EFF,
                    fp.DATA_FINE_EFF, fp.ORA_FINE_EFF,
                    fc.PZE01 AS woDonePieces, fc.PZE07 AS manualScrapedPz, 
                    fc.PZE08 AS startScrapedPz, fc.PZE09 AS automScrapedPz, fc.PZE10 AS scrapedPz
                FROM FINCOM fc
                LEFT JOIN FINPLAN fp ON fp.COMMESSA=fc.COMMESSA
                WHERE fc.COMMESSA IN {wo_to_fetch_secondary}
            """
            df_sec = self._fetch_legacy_table_with_schema(session, query_secondary, "FINCOM/FINPLAN")

            if not df_sec.is_empty():
                # Concatenazione date in Polars
                df_sec = df_sec.with_columns([
                    (pl.col("DATA_INIZIO_EFF").cast(pl.Utf8) + pl.col("ORA_INIZIO_EFF").cast(pl.Utf8)).alias("firstPieceTime"),
                    (pl.col("DATA_FINE_EFF").cast(pl.Utf8) + pl.col("ORA_FINE_EFF").cast(pl.Utf8)).alias("lastPieceTime")
                ])

                # Query complementari (Segrega e Magconf)
                df_seg = self._fetch_legacy_table_with_schema(session, f"SELECT COMMESSA as workOrder, sum(PEZZI_SEGREGATI) as totalSegrPz, sum(PEZZI_SCARTATI) as scrapedSegrPz FROM SEGREGA WHERE COMMESSA IN {wo_to_fetch_secondary} GROUP BY COMMESSA", "SEGREGA")
                df_mag = self._fetch_legacy_table_with_schema(session, f"SELECT COMMESSA as workOrder, sum(QT_CONTENUTA) as packedPz FROM MAGCONF WHERE COMMESSA IN {wo_to_fetch_secondary} GROUP BY COMMESSA", "MAGCONF")

                # Join finali in Polars
                df_agg_zero = df_sec.join(df_seg, on="workOrder", how="left").join(df_mag, on="workOrder", how="left")
                
                # Riempimento colonne mancanti e cast
                for col in STANDARD_COLUMNS:
                    if col not in df_agg_zero.columns:
                        df_agg_zero = df_agg_zero.with_columns(pl.lit(None if col in ['firstPieceTime', 'lastPieceTime'] else 0).alias(col))
                
                df_agg_zero = df_agg_zero.select(STANDARD_COLUMNS).with_columns([
                    pl.col(c).cast(pl.Int64).fill_null(0) for c in STANDARD_COLUMNS if c not in ['workOrder', 'firstPieceTime', 'lastPieceTime']
                ])

        # Unione finale
        return pl.concat([df_agg_pos.select(STANDARD_COLUMNS), df_agg_zero.select(STANDARD_COLUMNS)]).filter(pl.col("workOrder").is_not_null())

    


    def _fetch_datiturn_data(self, session: Session, work_orders_list: List[str]) -> pl.DataFrame:
        if not work_orders_list:
            return pl.DataFrame()
        
        wo_tuple = tuple(work_orders_list)
        segrega_fields = "0 as PEZZI_SEGR, 0 as PEZZI_SEGR_SCARTO," if self.plant == 'tn' else "PEZZI_SEGR, PEZZI_SEGR_SCARTO,"
        
        # 1. Query DATITURN: Scarichiamo le colonne RAW senza elaborazioni
        query = f"""
            SELECT COMMESSA AS workOrder, 
                PEZZI_BUONI, PZ_SCARTO_AVVIO, PZ_SCARTO_AUTOM, PEZZI_SCARTO,
                SCARTI_MAN, {segrega_fields} PEZZI_CONF,
                DATA_AGG_SYS, ORA_AGG_SYS
            FROM DATITURN WHERE COMMESSA IN {wo_tuple}
        """
        df_raw = self._fetch_legacy_table_with_schema(session, query, "DATITURN (raw)")

        STANDARD_COLUMNS = [
            'workOrder', 'firstPieceTime', 'lastPieceTime', 'woDonePieces', 
            'startScrapedPz', 'automScrapedPz', 'scrapedPz', 'manualScrapedPz', 
            'totalSegrPz', 'scrapedSegrPz', 'packedPz'
        ]
        
        # Inizializziamo i DataFrame per evitare errori di riferimento
        df_agg_pos = pl.DataFrame(schema={col: (pl.Utf8 if col in ['workOrder', 'firstPieceTime', 'lastPieceTime'] else pl.Int64) for col in STANDARD_COLUMNS})
        df_zero = pl.DataFrame()

        if not df_raw.is_empty():
            # Concatenazione rapida in Polars invece che in SQL
            df_raw = df_raw.with_columns(
                pl.when(pl.col("PEZZI_BUONI") > 0)
                .then(pl.col("DATA_AGG_SYS").cast(pl.Utf8) + pl.col("ORA_AGG_SYS").cast(pl.Utf8))
                .otherwise(None)
                .alias("pieceTime")
            ).rename({
                'PEZZI_SEGR': 'totalSegrPz', 'PEZZI_SEGR_SCARTO': 'scrapedSegrPz',
                'PZ_SCARTO_AVVIO': 'startScrapedPz', 'PZ_SCARTO_AUTOM': 'automScrapedPz',
                'PEZZI_SCARTO': 'scrapedPz', 'SCARTI_MAN': 'manualScrapedPz', 'PEZZI_CONF': 'packedPz'
            })
            
            df_pos = df_raw.filter(pl.col("PEZZI_BUONI") > 0)
            df_zero = df_raw.filter(pl.col("PEZZI_BUONI") == 0)

            if not df_pos.is_empty():
                df_agg_pos = df_pos.group_by('workOrder').agg(
                    pl.col('pieceTime').min().alias('firstPieceTime'),
                    pl.col('pieceTime').max().alias('lastPieceTime'),
                    pl.col('PEZZI_BUONI').sum().cast(pl.Int64).alias('woDonePieces'),
                    pl.col('startScrapedPz').sum().cast(pl.Int64),
                    pl.col('automScrapedPz').sum().cast(pl.Int64),
                    pl.col('scrapedPz').sum().cast(pl.Int64),
                    pl.col('manualScrapedPz').sum().cast(pl.Int64),
                    pl.col('totalSegrPz').sum().cast(pl.Int64),
                    pl.col('scrapedSegrPz').sum().cast(pl.Int64),
                    pl.col('packedPz').sum().cast(pl.Int64)
                )

        # 2. Gestione Work Orders mancanti o a zero
        wo_in_datiturn = set(df_raw.get_column('workOrder').unique().to_list()) if not df_raw.is_empty() else set()
        wo_missing = set(work_orders_list) - wo_in_datiturn
        wo_from_zero = set(df_zero.get_column('workOrder').to_list()) if not df_zero.is_empty() else set()
        
        wo_to_fetch_secondary = tuple(wo_missing | wo_from_zero)

        df_agg_zero = pl.DataFrame(schema=df_agg_pos.schema)
        if wo_to_fetch_secondary:
            # Anche qui scarichiamo le colonne RAW senza CONCAT
            query_secondary = f"""
                SELECT fc.COMMESSA AS workOrder,
                    fp.DATA_INIZIO_EFF, fp.ORA_INIZIO_EFF,
                    fp.DATA_FINE_EFF, fp.ORA_FINE_EFF,
                    fc.PZE01 AS woDonePieces, fc.PZE07 AS manualScrapedPz, 
                    fc.PZE08 AS startScrapedPz, fc.PZE09 AS automScrapedPz, fc.PZE10 AS scrapedPz
                FROM FINCOM fc
                LEFT JOIN FINPLAN fp ON fp.COMMESSA=fc.COMMESSA
                WHERE fc.COMMESSA IN {wo_to_fetch_secondary}
            """
            df_sec = self._fetch_legacy_table_with_schema(session, query_secondary, "FINCOM/FINPLAN")

            if not df_sec.is_empty():
                # Concatenazione date in Polars
                df_sec = df_sec.with_columns([
                    (pl.col("DATA_INIZIO_EFF").cast(pl.Utf8) + pl.col("ORA_INIZIO_EFF").cast(pl.Utf8)).alias("firstPieceTime"),
                    (pl.col("DATA_FINE_EFF").cast(pl.Utf8) + pl.col("ORA_FINE_EFF").cast(pl.Utf8)).alias("lastPieceTime")
                ])

                # Query complementari (Segrega e Magconf)
                df_seg = self._fetch_legacy_table_with_schema(session, f"SELECT COMMESSA as workOrder, CAST(SUM(PEZZI_SEGREGATI) AS DOUBLE) as totalSegrPz, sum(PEZZI_SCARTATI) as scrapedSegrPz FROM SEGREGA WHERE COMMESSA IN {wo_to_fetch_secondary} GROUP BY COMMESSA", "SEGREGA")
                df_mag = self._fetch_legacy_table_with_schema(session, f"SELECT COMMESSA as workOrder, CAST(SUM(QT_CONTENUTA) AS DOUBLE) as packedPz FROM MAGCONF WHERE COMMESSA IN {wo_to_fetch_secondary} GROUP BY COMMESSA", "MAGCONF")

                # Join finali in Polars
                df_agg_zero = df_sec.join(df_seg, on="workOrder", how="left").join(df_mag, on="workOrder", how="left")
                
                # Assicurati che tutte le colonne STANDARD_COLUMNS siano presenti
                for col in STANDARD_COLUMNS:
                    if col not in df_agg_zero.columns:
                        if col in ['workOrder', 'firstPieceTime', 'lastPieceTime']:
                            df_agg_zero = df_agg_zero.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
                        else:
                            df_agg_zero = df_agg_zero.with_columns(pl.lit(0, dtype=pl.Int64).alias(col))
                
                # MODIFICA: Gestione corretta della conversione dei tipi
                df_agg_zero = df_agg_zero.select(STANDARD_COLUMNS)
                
                # Applica le conversioni di tipo in modo condizionale
                for c in STANDARD_COLUMNS:
                    if c not in ['workOrder', 'firstPieceTime', 'lastPieceTime']:
                        # Controlla se la colonna è di tipo stringa
                        if df_agg_zero[c].dtype == pl.Utf8:
                            # Se è stringa, rimuovi spazi e converti
                            df_agg_zero = df_agg_zero.with_columns(
                                pl.col(c).str.strip_chars()
                                .cast(pl.Int64, strict=False)
                                .fill_null(0)
                                .alias(c)
                            )
                        else:
                            # Se è già numerica, converti direttamente
                            df_agg_zero = df_agg_zero.with_columns(
                                pl.col(c).cast(pl.Int64, strict=False)
                                .fill_null(0)
                                .alias(c)
                            )

        # Unione finale
        result_df = pl.concat([df_agg_pos.select(STANDARD_COLUMNS), df_agg_zero.select(STANDARD_COLUMNS)])
        return result_df.filter(pl.col("workOrder").is_not_null())
        






    def _fetch_comlis_fincom_data(self, session: Session, work_orders_list: List[str]) -> pl.DataFrame:
        if not work_orders_list: return pl.DataFrame()
        wo_tuple = tuple(work_orders_list)
        query = f"""
            SELECT COMMESSA AS workOrder, CICLO_STANDARD AS woCycle_source, CAVITA_STAMPO AS figures,
                   CAVITA_USATE AS usedFigures, TOTALE_PEZZI AS woTotPieces
            FROM COMLIS WHERE COMMESSA IN {wo_tuple}
            UNION ALL
            SELECT COMMESSA AS workOrder, CICLO_MEDIO AS woCycle_source, CAVITA_STAMPO AS figures,
                   CAVITA_USATE AS usedFigures, PEZZI_TOTALI AS woTotPieces
            FROM FINCOM WHERE COMMESSA IN {wo_tuple}
        """

        df = self._fetch_legacy_table_with_schema(session, query, "COMLIS/FINCOM")
        return df.unique(subset=['workOrder'], keep='first')
    
    def _fetch_jobsetup_data(self, session: Session, work_orders_list: List[str]) -> pl.DataFrame:
        if not work_orders_list: 
            return pl.DataFrame()
        
        wo_tuple = tuple(work_orders_list)
        query = f"""
            SELECT j.COMMESSA as workOrder, j.NUMERO_PROGRAMMA as program, o.DATA_REGISTRAZIONE as progRegDate
            FROM JOBSETUP j
            LEFT JOIN OFFARB o ON j.NUMERO_PROGRAMMA = o.NUMERO_PROGRAMMA
            WHERE j.COMMESSA IN {wo_tuple}
        """
        df = self._fetch_legacy_table_with_schema(session, query, "JOBSETUP/OFFARB")
        if df.is_empty():
            return pl.DataFrame(schema=self._get_jobsetup_schema())
        
        return df.unique(subset=['workOrder'])

    def _get_date_range(self) -> Tuple[datetime, datetime]:
        max_date_result = None
        try:
            with db_manager.get_sessions() as (_, mysql_session):
                query = text("SELECT MAX(woStart) FROM woStatic_tbl WHERE plant = :plant AND woStart <= NOW()")
                max_date_result = mysql_session.execute(query, {"plant": self.plant}).scalar_one_or_none()
        except Exception as e:
            logger.warning(f"Impossibile recuperare max_date. Uso data di default. Errore: {e}")
        
        if max_date_result:
            reference_date = max_date_result
            start_date = reference_date - timedelta(days=60)
            end_date = reference_date + timedelta(days=30)
        else:
            logger.warning("Tabella woStatic_tbl vuota o senza date valide. Calcolo intervallo di fallback.")
            start_date = datetime(1989, 1, 12)
            if(PLANT=='tn'):
                start_date = datetime(2009, 7, 8)
            elif(PLANT=='pl'):
                start_date = datetime(2008, 2, 27)
            end_date = start_date + timedelta(days=30)

        return start_date, end_date

    def _normalize_datetime_field(self, date_col: str, time_col: str) -> pl.Expr:
        """
        Funzione di supporto per normalizzare e convertire campi di data e ora.
        """
        normalized_time = pl.col(time_col).cast(pl.Utf8, strict=False).str.zfill(4)
        datetime_string = pl.concat_str([pl.col(date_col), normalized_time], separator="")
        return datetime_string.str.strptime(pl.Datetime, format="%Y%m%d%H%M", strict=False)

    def _normalize_wo_data(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df.clear()

        expected_cols = {
            'workOrder': pl.Utf8, 'press': pl.Utf8, 'mold': pl.Utf8, 'article': pl.Utf8,
            'cycle': pl.Float64, 'state': pl.Utf8, 'woTest': pl.Utf8,
            'woStart_date': pl.Utf8, 'woStart_time': pl.Utf8, 'woEnd_date': pl.Utf8,
            'woEnd_time': pl.Utf8, 'firstPieceTime': pl.Utf8, 'lastPieceTime': pl.Utf8,
            'program': pl.Utf8, 'progRegDate': pl.Date,
            'woCycle_source': pl.Float64, 'figures': pl.Int64, 'usedFigures': pl.Int64,
            'woTotPieces': pl.Int64, 'woDonePieces': pl.Int64, 'startScrapedPz': pl.Int64,
            'automScrapedPz': pl.Int64, 'scrapedPz': pl.Int64, 'manualScrapedPz': pl.Int64,
            'totalSegrPz': pl.Int64, 'scrapedSegrPz': pl.Int64, 'packedPz': pl.Int64
        }
        
        for col, dtype in expected_cols.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
        
        df = df.with_columns([
            pl.col("workOrder").str.slice(0, 10).str.strip_chars().alias("workOrder"),
            pl.col('article').str.strip_chars(),
            pl.when(pl.col("woTest").is_in(['PR', 'S']))
                .then(pl.lit('S'))
                .otherwise(pl.lit('N'))
                .alias("woTest"),
            pl.when(pl.col("state") == 'A').then(pl.lit('Pianificata'))
                .when(pl.col("state") == 'P').then(pl.lit('In Lavorazione'))
                .when(pl.col("state") == 'F').then(pl.lit('Finito'))
                .when(pl.col("state") == 'S').then(pl.lit('Sospeso'))
                .otherwise(pl.col("state"))
                .alias("woState"),
            pl.lit(self.plant).alias("plant")
        ])

        integer_cols = [
            'figures', 'usedFigures', 'woTotPieces', 'woDonePieces', 
            'startScrapedPz', 'automScrapedPz', 'scrapedPz', 'manualScrapedPz', 
            'totalSegrPz', 'scrapedSegrPz', 'packedPz'
        ]
        
        float_cols = ['cycle', 'woCycle_source']

        for col in integer_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False).fill_null(0).alias(col))

        for col in float_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0).alias(col))

        df = df.with_columns(
            pl.when(pl.col('woCycle_source').is_not_null())
                .then(pl.col('woCycle_source'))
                .otherwise(pl.col('cycle'))
                .fill_null(0.0)
                .alias('woCycle')
        )
        
        df = df.with_columns(
            pl.when(pl.col('cycle').is_not_null())
                .then(pl.col('cycle'))
                .otherwise(pl.col('woCycle'))
                .fill_null(0.0)
                .alias('cycle')
        )
        
        MAX_INT = 2_147_483_647
        df = df.with_columns(
            pl.when(pl.col('woTotPieces').cast(pl.Int64, strict=False) > MAX_INT)
                .then(pl.lit(MAX_INT, dtype=pl.Int64))
                .otherwise(pl.col('woTotPieces').cast(pl.Int64, strict=False))
                .alias('woTotPieces')
        )

        df = df.with_columns([
            self._normalize_datetime_field('woStart_date', 'woStart_time').alias("woStart"),
            self._normalize_datetime_field('woEnd_date', 'woEnd_time').alias("woEnd"),
            pl.col('firstPieceTime').str.strptime(pl.Datetime, format="%Y%m%d%H%M%S", strict=False).alias("firstPieceTime"),
            pl.col('lastPieceTime').str.strptime(pl.Datetime, format="%Y%m%d%H%M%S", strict=False).alias("lastPieceTime"),            
            pl.col("progRegDate").cast(pl.String).str.strptime(pl.Date, format="%Y%m%d", strict=False).alias("progRegDate")
        ])

        df = df.with_columns([
            pl.when(pl.col("firstPieceTime").is_not_null()).then(pl.col("firstPieceTime")).otherwise(pl.col("woStart")).alias("woStart"),
            pl.when(pl.col("woState") == 'Finito')
            .then(
                pl.when(pl.col("lastPieceTime").is_not_null())
                .then(pl.col("lastPieceTime"))
                .otherwise(pl.col("woEnd"))
            )
            .otherwise(pl.col("woEnd"))
            .alias("woEnd")
        ])

        cols_to_drop = [
            'state', 'woStart_date', 'woStart_time', 'woEnd_date', 'woEnd_time', 
            'firstPieceTime', 'lastPieceTime', 'woCycle_source'
        ]
        df = df.drop([c for c in cols_to_drop if c in df.columns])

        return df

    
    
    def _resolve_foreign_keys(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Versione Corretta: Risolve il problema del drop su colonna inesistente dopo il join.
        """
        self._load_lookup_caches()
        
        # 1. Prima di fare join, assicuriamoci che 'cycle' esista e non sia 0.0
        # Se cycle è 0.0, proviamo a usare woCycle come fallback iniziale
        if 'cycle' in df.columns:
            df = df.with_columns([
                pl.when((pl.col('cycle') == 0.0) | pl.col('cycle').is_null())
                    .then(pl.col('woCycle'))
                    .otherwise(pl.col('cycle'))
                    .alias('cycle_temp')
            ])
            df = df.drop('cycle').rename({'cycle_temp': 'cycle'})
        
        # 2. Join standard - RINOMINA la colonna 'cycle' della tabella stampi per evitare conflitti
        df = df.join(self._presses_pl, on='press', how='left')
        
        # Rinominiamo esplicitamente 'cycle' da molds_pl a 'cycle_mold'
        molds_with_renamed_cycle = self._molds_pl.rename({'cycle': 'cycle_mold'})
        df = df.join(molds_with_renamed_cycle, on='mold', how='left')
        
        df = df.join(self._articles_pl, on='article', how='left')
        
        # 3. Fallback: se cycle è 0.0 o null, usa cycle_mold (dalla tabella stampi)
        df = df.with_columns([
            pl.when((pl.col('cycle') == 0.0) | pl.col('cycle').is_null())
                .then(pl.col('cycle_mold'))
                .otherwise(pl.col('cycle'))
                .alias('cycle')
        ])
        
        # 4. Rimuovi colonna temporanea
        if 'cycle_mold' in df.columns:
            df = df.drop('cycle_mold')
        
        # 5. Join Fallback Articolo tramite idMold
        df = df.join(self._articles_fallback_pl, left_on='idMold', right_on='idMold_ref', how='left')
        
        # 6. Coalesce e pulizia
        df = df.with_columns(
            pl.coalesce([pl.col('idArticle'), pl.col('idArticle_fallback')]).alias('idArticle')
        ).drop(['idArticle_fallback'])
        
        # 7. Risoluzione idCustomer (Priorità Articolo, poi Stampo)
        df = df.with_columns(
            pl.coalesce([pl.col('idCustomerArt'), pl.col('idCustomerMold')]).alias('idCustomer')
        ).drop(['idCustomerArt', 'idCustomerMold'])
        
        return df




    def _load_lookup_caches(self):
        """
        Versione Corretta: Include il campo 'cycle' dalla tabella stampi.
        """
        if hasattr(self, '_presses_pl'):
            return

        with db_manager.get_sessions() as (_, mysql_session):
            # Cache base (Presse, Stampi, Articoli)
            # CORREZIONE: Aggiungi 'cycle' alla query degli stampi
            self._presses_pl = pl.read_database(
                f"SELECT press, idPress FROM presses_tbl WHERE plant = '{self.plant}'", 
                mysql_session.bind
            )
            self._molds_pl = pl.read_database(
                f"SELECT mold, idMold, idOwnerCode as idCustomerMold, cycle FROM moldStatic_tbl WHERE plant = '{self.plant}'", 
                mysql_session.bind
            )
            self._articles_pl = pl.read_database(
                f"SELECT article, idArticle, idCustomer as idCustomerArt FROM articles_tbl WHERE plant = '{self.plant}'", 
                mysql_session.bind
            )
            
            # Cache Fallback Articoli
            art_molds_raw = pl.read_database(
                f"SELECT idArticle, idMold, idMold2, idMold3, idMold4 FROM articles_tbl WHERE plant = '{self.plant}'", 
                mysql_session.bind
            )
            
            self._articles_fallback_pl = (
                art_molds_raw.unpivot(
                    index=["idArticle"], 
                    on=["idMold", "idMold2", "idMold3", "idMold4"],
                    variable_name="mold_origin",
                    value_name="idMold_ref"
                )
                .filter(pl.col("idMold_ref").is_not_null())
                .rename({"idArticle": "idArticle_fallback"})
                .unique(subset=["idMold_ref"], keep="first")
                .select(["idMold_ref", "idArticle_fallback"])
            )


    

    def _validate_wo_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Valida i dati dei work order, traccia le discrepanze ma NON rimuove i record.
        Aggiorna bPortingError e portingErrorDesc per i record problematici.
        """
        if df.is_empty(): 
            return df
        
        original_count = len(df)
        
        # Gestione workOrder nullo o vuoto - questi DEVONO essere rimossi
        removed_null = df.filter(pl.col('workOrder').is_null())
        if not removed_null.is_empty():
            for row in removed_null.iter_rows(named=True):
                self.discrepancy_tracker.add_discrepancy(
                    'WORKORDER_NULL',
                    workorder='NULL',
                    press=row.get('press'),
                    mold=row.get('mold'),
                    article=row.get('article'),
                    reason='workOrder is null - RECORD SCARTATO'
                )
        df = df.filter(pl.col('workOrder').is_not_null())
        
        removed_empty = df.filter(pl.col('workOrder') == '')
        if not removed_empty.is_empty():
            for row in removed_empty.iter_rows(named=True):
                self.discrepancy_tracker.add_discrepancy(
                    'WORKORDER_EMPTY',
                    workorder='EMPTY',
                    press=row.get('press'),
                    mold=row.get('mold'),
                    article=row.get('article'),
                    reason='workOrder is empty string - RECORD SCARTATO'
                )
        df = df.filter(pl.col('workOrder') != '')
        
        # FIX CRITICO: Aggiungi il return statement mancante!
        return df




    def _clean_current_df(self, df: pl.DataFrame) -> pl.DataFrame:
        cols_to_drop = ['idWoDyn', 'lastModified']
        df = df.drop([c for c in cols_to_drop if c in df.columns])
        return df
    
    def _execute_multi_table_operations_on_changes(self, to_insert: pl.DataFrame, to_update: pl.DataFrame, session: Session) -> Tuple[int, int]:
        metadata = MetaData()
        metadata.reflect(bind=session.bind)
        static_table = Table('woStatic_tbl', metadata, autoload_with=session.bind)
        dynamic_table = Table('woDynamic_tbl', metadata, autoload_with=session.bind)
        
        inserted_count = self._execute_inserts(to_insert, static_table, dynamic_table, session)
        updated_count = self._execute_updates(to_update, static_table, dynamic_table, session)
        
        return inserted_count, updated_count

    def _execute_multi_table_deletes(self, to_delete: pl.DataFrame, session: Session) -> int:
        if to_delete.is_empty():
            return 0
        
        metadata = MetaData()
        metadata.reflect(bind=session.bind)
        static_table = Table('woStatic_tbl', metadata, autoload_with=session.bind)
        dynamic_table = Table('woDynamic_tbl', metadata, autoload_with=session.bind)
        
        wo_codes_to_delete = to_delete.get_column('workOrder').unique().to_list()
        if not wo_codes_to_delete:
            return 0
        
        #print(f"Trovate {len(wo_codes_to_delete)} commesse da eliminare: {', '.join(wo_codes_to_delete)}")
        
        id_query = text("SELECT idWorkOrder FROM woStatic_tbl WHERE workOrder IN :codes AND plant = :plant")
        id_list = [row[0] for row in session.execute(id_query, {'codes': tuple(wo_codes_to_delete), 'plant': self.plant})]
        
        if not id_list:
            logger.warning("Nessun ID corrispondente trovato in MySQL per le commesse da eliminare.")
            return 0
        
        deleted_dyn = session.execute(dynamic_table.delete().where(dynamic_table.c.idWorkOrder.in_(id_list))).rowcount
        #print(f"Eliminati {deleted_dyn} record da woDynamic_tbl.")
        
        deleted_static = session.execute(static_table.delete().where(static_table.c.idWorkOrder.in_(id_list))).rowcount
        #print(f"Eliminati {deleted_static} record da woStatic_tbl.")
        
        return deleted_static

    def _execute_inserts(self, df: pl.DataFrame, static_table: Table, dynamic_table: Table, session: Session) -> int:
        """
        Esegue gli inserimenti includendo i campi bPortingError e portingErrorDesc
        """
        if df.is_empty(): 
            return 0
        
        inserted_count = 0
        static_cols = {c.name for c in static_table.columns}
        dynamic_cols = {c.name for c in dynamic_table.columns}
        
        for record in df.to_dicts():
            static_data = {k.replace('_legacy', ''): v for k, v in record.items() if k.replace('_legacy', '') in static_cols}
            for pk in self.get_primary_key_columns():
                if pk in record: 
                    static_data[pk] = record[pk]
            
            # Aggiungi i campi di errore
            static_data['bPortingError'] = record.get('bPortingError', 0)
            static_data['portingErrorDesc'] = record.get('portingErrorDesc', '')
            static_data['lastModified'] = self.formatted_datetime
            
            try:
                res = session.execute(static_table.insert().values(self._sanitize_for_mysql(static_data)))
                id_wo = res.inserted_primary_key[0]
                
                dynamic_data = {k.replace('_legacy', ''): v for k, v in record.items() if k.replace('_legacy', '') in dynamic_cols}
                dynamic_data['idWorkOrder'] = id_wo
                dynamic_data['bPortingError'] = record.get('bPortingError', 0)
                dynamic_data['portingErrorDesc'] = record.get('portingErrorDesc', '')
                dynamic_data['lastModified'] = self.formatted_datetime
                dynamic_data.pop('workOrder', None)
                
                if dynamic_data.get('woCycle') is None:
                    logger.warning(f"workOrder '{record.get('workOrder', 'N/A')}' ha woCycle nullo. Inserito valore default 0.0.")
                    dynamic_data['woCycle'] = 0.0

                session.execute(dynamic_table.insert().values(self._sanitize_for_mysql(dynamic_data)))
                inserted_count += 1
                
                # Log se inserito con errori
                if record.get('bPortingError', 0) == 1:
                    logger.info(f"Inserito record con errori - WO: {record.get('workOrder')}, Errori: {record.get('portingErrorDesc')}")
                    
            except Exception as e:
                logger.error(f"Errore durante l'inserimento del record {record.get('workOrder', 'N/A')}: {e}")
        
        return inserted_count

    def _execute_updates(self, df: pl.DataFrame, static_table: Table, dynamic_table: Table, session: Session) -> int:
        """
        Esegue gli aggiornamenti includendo i campi bPortingError e portingErrorDesc
        """
        if df.is_empty(): 
            return 0
        
        updated_count = 0
        static_cols = {c.name for c in static_table.columns}
        dynamic_cols = {c.name for c in dynamic_table.columns}
        
        comparison_cols = self.get_comparison_columns()
        
        for record in df.to_dicts():
            pk_val = record['workOrder']
            id_wo = session.execute(
                text("SELECT idWorkOrder FROM woStatic_tbl WHERE workOrder = :wo AND plant = :plant"), 
                {'wo': pk_val, 'plant': self.plant}
            ).scalar()
            
            if not id_wo: 
                continue

            changed_fields_log = []
            for col in comparison_cols:
                legacy_val = record.get(f'{col}_legacy')
                current_val = record.get(f'{col}_current')
                
                if not self._compare_robustly(legacy_val, current_val):
                    changed_fields_log.append(f"  - Campo '{col}': '{current_val}' -> '{legacy_val}'")
            
            '''if changed_fields_log:
                #print(f"Modifiche per Work Order '{pk_val}':")
                for log_line in changed_fields_log:
                    print(log_line)'''
            
            static_data = {k.replace('_legacy', ''): v for k, v in record.items() 
                          if k.endswith('_legacy') and k.replace('_legacy', '') in static_cols}
            
            if static_data or 'bPortingError' in record:
                # Aggiungi sempre i campi di errore negli update
                if 'bPortingError' in record:
                    static_data['bPortingError'] = record['bPortingError']
                if 'portingErrorDesc' in record:
                    static_data['portingErrorDesc'] = record['portingErrorDesc']
                
                static_data['lastModified'] = self.formatted_datetime
                session.execute(
                    static_table.update()
                    .where(static_table.c.idWorkOrder == id_wo)
                    .values(self._sanitize_for_mysql(static_data))
                )
            
            dynamic_data = {k.replace('_legacy', ''): v for k, v in record.items() 
                           if k.endswith('_legacy') and k.replace('_legacy', '') in dynamic_cols}
            
            if dynamic_data or 'bPortingError' in record:
                if 'bPortingError' in record:
                    dynamic_data['bPortingError'] = record['bPortingError']
                if 'portingErrorDesc' in record:
                    dynamic_data['portingErrorDesc'] = record['portingErrorDesc']
                
                dynamic_data['lastModified'] = self.formatted_datetime
                dynamic_data.pop('idWorkOrder', None)
                session.execute(
                    dynamic_table.update()
                    .where(dynamic_table.c.idWorkOrder == id_wo)
                    .values(self._sanitize_for_mysql(dynamic_data))
                )
            
            updated_count += 1
            
        return updated_count
   
    def _compare_robustly(self, val1, val2):
        if val1 is None and val2 is None:
            return True
        if val1 is None or val2 is None:
            return False
        
        str1 = str(val1).strip()
        str2 = str(val2).strip()

        try:
            float1 = float(str1)
            float2 = float(str2)
            if abs(float1 - float2) < 1e-9:
                return True
        except (ValueError, TypeError):
            pass
            
        return str1 == str2

    def _sanitize_for_mysql(self, data_dict: dict) -> dict:
        sanitized = {}
        for key, value in data_dict.items():
            if value is None or (isinstance(value, float) and value != value):
                sanitized[key] = None
            else:
                sanitized[key] = value
        return sanitized


# ======================================================================================
# Funzioni di entry point
# ======================================================================================

def check_future_work_orders(plant: str) -> bool:
    """
    Verifica se esistono work order con woStart futura rispetto alla data attuale.
    """
    try:
        with db_manager.get_sessions() as (_, mysql_session):
            query = text("""
                SELECT COUNT(*) as count_future
                FROM woStatic_tbl 
                WHERE plant = :plant AND woStart > NOW()
            """)
            result = mysql_session.execute(query, {"plant": plant}).scalar_one()
            return result > 0
    except Exception as e:
        logger.error(f"Errore nel controllo work order futuri: {e}")
        return False

def get_next_future_wostart(plant: str) -> datetime:
    """
    Ottiene la prossima data woStart futura.
    """
    try:
        with db_manager.get_sessions() as (_, mysql_session):
            query = text("""
                SELECT MIN(woStart) as next_wostart
                FROM woStatic_tbl 
                WHERE plant = :plant AND woStart > NOW()
            """)
            result = mysql_session.execute(query, {"plant": plant}).scalar_one_or_none()
            return result
    except Exception as e:
        logger.error(f"Errore nel recupero prossima data woStart: {e}")
        return None

def updateWorkOrders(plant: str) -> dict:
    """
    Funzione per l'aggiornamento delle tabelle woStatic_tbl e woDynamic_tbl.
    """
    with timer.Timer(""):
        if plant is None:
            plant = PLANT

        try:
            manager = WorkOrderSyncManager(plant)
            logger.info(f"🚀 AVVIO SINCRONIZZAZIONE WORKORDERS:")
            
            inserted, updated, deleted = manager.synchronize()
            
            #print(f"Sincronizzazione completata: {inserted} inseriti, {updated} aggiornati, {deleted} eliminati.")
            return {'status': 'success', 'inserted': inserted, 'updated': updated, 'deleted': deleted}
            
        except Exception as e:
            logger.error(f"Errore critico durante la sincronizzazione WorkOrders per il plant '{plant}': {e}", exc_info=True)
            return {'status': 'failed', 'error_message': str(e)}

def main() -> None:
    """
    Funzione principale del programma.
    Esegue la sincronizzazione e stampa il risultato in formato JSON.
    """
    try:
        result = updateWorkOrders(PLANT)
        
        if result.get('status') == 'success':
            result_data = {
                "status": "completed",
                "message": "Sincronizzazione completata con successo",
                "inserted": result.get('inserted', 0),
                "updated": result.get('updated', 0),
                "deleted": result.get('deleted', 0)
            }
        else:
            result_data = {
                "status": "failed",
                "message": result.get('error_message', 'Errore sconosciuto'),
                "inserted": 0,
                "updated": 0,
                "deleted": 0
            }
        
        print(json.dumps(result_data))
        
    except KeyboardInterrupt:
        result_data = {
            "status": "failed",
            "message": "Processo interrotto dall'utente",
            "inserted": 0, "updated": 0, "deleted": 0
        }
        print(json.dumps(result_data))
        sys.exit(1)
        
    except Exception as e:
        result_data = {
            "status": "failed",
            "message": f"Errore critico non gestito: {str(e)}",
            "inserted": 0, "updated": 0, "deleted": 0
        }
        print(json.dumps(result_data))
        sys.exit(1)


def main_recursive() -> None:
    max_iterations = 150
    iteration_count = 0
    sleep_time = 10
    
    try:
        while iteration_count < max_iterations:
            iteration_count += 1
            current_time = datetime.now()
            
            print(f"=== ITERAZIONE {iteration_count} - {current_time.strftime('%Y-%m-%d %H:%M:%S')}   plant: {PLANT} ===")
            
            try:
                result = updateWorkOrders(PLANT)
                print(f"Risultato sincronizzazione: {result}")
                
                if result.get('status') == 'failed':
                    print(f"Sincronizzazione fallita: {result.get('error_message')}")
                
            except Exception as e:
                print(f"Errore durante la sincronizzazione nell'iterazione {iteration_count}: {e}", exc_info=True)
            
            has_future_wo = check_future_work_orders(PLANT)
            
            if has_future_wo:
                print("Tutti i work order hanno woStart <= data attuale. Processo completato.")
                break
            
            next_wostart = get_next_future_wostart(PLANT)
            if next_wostart:
                time_until_next = (next_wostart - current_time).total_seconds()
                print(f"Prossimo work order inizia il: {next_wostart.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Tempo rimanente: {time_until_next/3600:.1f} ore")
                
                if time_until_next > sleep_time:
                    print(f"Attesa di {sleep_time/60:.0f} minuti prima della prossima iterazione...")
                    time.sleep(sleep_time)
                else:
                    wait_time = max(60, time_until_next - 300)
                    print(f"Attesa di {wait_time/60:.1f} minuti (prossimo WO vicino)...")
                    time.sleep(wait_time)
            else:
                print(f"Attesa di {sleep_time/60:.0f} minuti prima della prossima iterazione...")
                time.sleep(sleep_time)
        
        if iteration_count >= max_iterations:
            print(f"Raggiunto limite massimo di iterazioni ({max_iterations}). Processo interrotto.")
        
        print("Processo principale terminato.")
        
    except KeyboardInterrupt:
        print("Processo interrotto manualmente dall'utente.")
    except Exception as e:
        print(f"Errore critico non gestito nel main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--recursive':
        main_recursive()
    else:
        #profiler = Profiler()
        #profiler.start()
        main()
        #profiler.stop()
        #print(profiler.output_text(unicode=True, color=True))