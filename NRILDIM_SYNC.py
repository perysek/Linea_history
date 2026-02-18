"""
nRildim Synchronization Manager - Versione con Gestione Errori di Porting

Modifiche principali:
- I record con errori vengono SEMPRE inseriti
- Campo bPortingError = 1 per record con problemi
- Campo portingErrorDesc contiene la descrizione dell'errore
- I campi FK mancanti vengono lasciati a NULL invece di 0
"""

import sys
import json
import logging
import polars as pl
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from collections import defaultdict
from pathlib import Path

# Importazioni locali
from constants import PLANT
from DatabaseManager import db_manager
from BaseSyncManager import BaseSyncManager
from Decorators import timer
from Functions import functions


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class nRildimSyncManager(BaseSyncManager):
    """
    Gestore specifico per la sincronizzazione delle misure dimensionali
    con gestione errori di porting.
    """
    
    def __init__(self, plant: str):
        """Inizializza il gestore delle misure dimensionali."""
        super().__init__(plant, 'nrildim_tbl')
        self._workorder_cache: Dict[str, int] = {}
        self._press_cache: Dict[str, int] = {}
        self._mold_cache: Dict[str, int] = {}
        self._article_cache: Dict[str, int] = {}
        self.date_range: Tuple[datetime, datetime] = self._get_date_range()
        
        # Contatori di debug
        self.debug_stats = {
            'finplan_workorders_found': 0,
            'raw_legacy_records': 0,
            'after_basic_transform': 0,
            'after_datetime_filter': 0,
            'records_with_errors': 0,
            'records_without_errors': 0,
            'final_records': 0,
        }

        # Dizionario per tracciare discrepanze (ora solo per statistiche)
        self.discrepancies: Dict[tuple, int] = defaultdict(int)
    
    def _get_date_range(self) -> Tuple[datetime, datetime]:
        """Determina il range di date per la sincronizzazione."""
        try:
            with db_manager.get_sessions() as (_, mysql_session):
                query = text("""
                    SELECT MAX(measureDateTime) as max_date 
                    FROM nrildim_tbl 
                    WHERE plant = :plant
                """)
                result = mysql_session.execute(query, {"plant": self.plant}).scalar_one_or_none()
                
                if result is not None:
                    start_date = result
                    end_date = start_date + timedelta(days=30)
                    print(f"Trovato ultimo record: {result}. Range: {start_date} - {end_date}")
                else:
                    print("Nessun record esistente, uso date di default")
                    if self.plant == 'it':
                        start_date = datetime(1998, 10, 23)
                    elif self.plant == 'tn':
                        start_date = datetime(2009, 7, 8)
                    elif self.plant == 'pl':
                        start_date = datetime(2008, 9, 29)
                    else:
                        start_date = datetime(2005, 1, 1)
                    
                    end_date = start_date + timedelta(days=30)
                        
        except Exception as e:
            logger.warning(f"Errore nel calcolo date range: {e}. Uso date di default.")
            start_date = datetime(2005, 1, 1)
            end_date = start_date + timedelta(days=30)
        
        return start_date, end_date
    
    def update_date_range(self, start_date: datetime, end_date: datetime):
        """Aggiorna il range temporale per la sincronizzazione."""
        self.date_range = (start_date, end_date)
        logger.info(f"Range temporale aggiornato: {start_date} - {end_date}")
    
    def get_legacy_query(self) -> str:
        """Query gestita dinamicamente in fetch_legacy_data."""
        return "-- Query gestita dinamicamente in fetch_legacy_data"
    
    def get_primary_key_columns(self) -> List[str]:
        """Restituisce le colonne della chiave primaria logica."""
        return ['referenceNum', 'measureDateTime', 'numPrint', 'numFigure', 'plant']
        
    def get_comparison_columns(self) -> List[str]:
        """Restituisce le colonne da confrontare per identificare modifiche."""
        return [
            'idWorkOrder', 'idPress', 'idMold', 'idArticle',
            'measureDateTime', 'operator', 'numPrint', 'numFigure', 'measure',
            'bPortingError', 'portingErrorDesc'
        ]
    
    def get_legacy_column_types(self) -> Dict[str, pl.DataType]:
        """Definisce i tipi delle colonne legacy."""
        types = {
            'article': pl.String, 'mold': pl.String, 'press': pl.String,
            'workOrder': pl.String, 'operator': pl.String, 'measureDate': pl.String,
            'measureHour': pl.String, 'referenceNum': pl.String,
            'numPrint': pl.String, 'numFigure': pl.String,
        }
        
        for i in range(1, 21):
            types[f'mis{i:02d}'] = pl.String
        
        return types

    def _bulk_update(self, to_update: pl.DataFrame, mysql_session: Session) -> int:
        """Override: per questo sync manager non facciamo update."""
        if not to_update.is_empty():
            logger.debug(f"SKIP {len(to_update)} operazioni UPDATE (disabilitate)")
        return 0
    
    def _bulk_delete(self, to_delete: pl.DataFrame, mysql_session: Session) -> int:
        """Override: per questo sync manager non facciamo delete."""
        if not to_delete.is_empty():
            logger.debug(f"SKIP {len(to_delete)} operazioni DELETE (disabilitate)")
        return 0

    def fetch_current_data(self, mysql_session: Session) -> pl.DataFrame:
        """Override ottimizzato che carica solo i record nel range temporale corrente."""
        try:
            start_date, end_date = self.date_range
            
            query = text(f"""
                SELECT * 
                FROM {self.table_name} 
                WHERE plant = :plant
                AND measureDateTime >= :start_date
                AND measureDateTime < :end_date
            """)
            
            result = mysql_session.execute(
                query, 
                {
                    "plant": self.plant,
                    "start_date": start_date,
                    "end_date": end_date
                }
            )
            rows = result.fetchall()

            logger.debug(f"Record attuali in MySQL per plant {self.plant} nel range {start_date} - {end_date}: {len(rows)}")

            if not rows:
                logger.info(f"Nessun record attuale per {self.table_name} nel range specificato")
                return pl.DataFrame()

            column_names = list(result.keys())
            df = pl.DataFrame(rows, schema=column_names, orient="row")

            if 'measureDateTime' in df.columns:
                df = df.with_columns(
                    pl.col('measureDateTime').cast(pl.Datetime("ms")).alias('measureDateTime')
                )

            columns_to_remove = ['lastModified', 'idRilDim']
            df = df.drop([col for col in columns_to_remove if col in df.columns])

            logger.info(f"Recuperati {len(df)} record attuali per {self.table_name} nel range temporale")
            return df

        except Exception as e:
            logger.error(f"Errore durante il recupero dati attuali per {self.table_name}: {str(e)}")
            raise

    def compare_datasets(self, legacy_df: pl.DataFrame, current_df: pl.DataFrame) -> pl.DataFrame:
        """Override con debug per il confronto dataset."""
        print(f"Confronto dataset - Legacy: {len(legacy_df)}, Current: {len(current_df)}")
        
        primary_keys = self.get_primary_key_columns()
        comparison_columns = self.get_comparison_columns()

        if legacy_df.is_empty():
            if current_df.is_empty():
                return pl.DataFrame()
            
            cols_to_rename = [c for c in current_df.columns if c not in primary_keys]
            rename_map = {c: f"{c}_current" for c in cols_to_rename}
            result = current_df.rename(rename_map).with_columns(
                pl.lit("right_only").alias("_merge")
            )
            return result

        if current_df.is_empty():            
            cols_to_rename = [c for c in legacy_df.columns if c not in primary_keys]
            rename_map = {c: f"{c}_legacy" for c in cols_to_rename}
            result = legacy_df.rename(rename_map).with_columns(
                pl.lit("left_only").alias("_merge")
            )

            logger.info(f"Confronto completato: {len(current_df)} record analizzati")
            return result

        legacy_cols_to_rename = [c for c in comparison_columns if c in legacy_df.columns and c not in primary_keys]
        current_cols_to_rename = [c for c in comparison_columns if c in current_df.columns and c not in primary_keys]
        
        legacy_renamed = legacy_df.rename({c: f"{c}_legacy" for c in legacy_cols_to_rename})
        current_renamed = current_df.rename({c: f"{c}_current" for c in current_cols_to_rename})
        
        merged_df = legacy_renamed.join(
            current_renamed,
            on=primary_keys,
            how='full'
        )

        ref_col_legacy = f"{legacy_cols_to_rename[0]}_legacy" if legacy_cols_to_rename else primary_keys[0]
        ref_col_current = f"{current_cols_to_rename[0]}_current" if current_cols_to_rename else primary_keys[0]

        merge_indicator_expr = (
            pl.when(pl.col(ref_col_current).is_null())
            .then(pl.lit('left_only'))
            .when(pl.col(ref_col_legacy).is_null())
            .then(pl.lit('right_only'))
            .otherwise(pl.lit('both'))
            .alias('_merge')
        )

        merged_df = merged_df.with_columns(merge_indicator_expr)
        
        logger.info(f"Confronto completato: {len(merged_df)} record analizzati")
        return merged_df

    def identify_changes(self, merged_df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Override con debug per identificazione modifiche."""
        print(f"Identificazione modifiche da {len(merged_df)} record merged")
        
        try:
            if merged_df.is_empty():
                return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
            
            to_insert = merged_df.filter(pl.col('_merge') == 'left_only')
            to_delete = merged_df.filter(
                (pl.col('_merge') == 'right_only') & 
                (pl.col('plant') == self.plant)
            )
            both_records = merged_df.filter(pl.col('_merge') == 'both')
            to_update = self._identify_updated_records(both_records)
            
            logger.error(f"Changes identified: Insert={len(to_insert)}, Update={len(to_update)}, Delete={len(to_delete)}")
            
            return to_insert, to_update, to_delete
            
        except Exception as e:
            print(f"Errore nell'identificazione delle modifiche per {self.table_name}: {str(e)}")
            raise

    def _bulk_insert(self, to_insert: pl.DataFrame, mysql_session: Session) -> int:
        """Override OTTIMIZZATO con inserimento bulk in batch."""
        if to_insert.is_empty():
            print("Nessun record da inserire")
            return 0
        
        logger.info(f"Preparazione inserimento di {len(to_insert)} record")
        
        try:
            insert_data = self._prepare_insert_data(to_insert)
            
            if not insert_data:
                logger.warning("ATTENZIONE - Nessun dato preparato per inserimento!")
                return 0
            
            # Aggiungi plant e lastModified a tutti i record
            for record in insert_data:
                record['plant'] = self.plant
                record['lastModified'] = self.formatted_datetime
            
            from sqlalchemy import MetaData, Table
            metadata = MetaData()
            metadata.reflect(bind=mysql_session.bind)
            table = Table(self.table_name, metadata, autoload=True)
            
            # INSERIMENTO BULK IN BATCH
            batch_size = 10000
            total_inserted = 0
            errors = 0
            
            for i in range(0, len(insert_data), batch_size):
                batch = insert_data[i:i + batch_size]
                
                try:
                    insert_stmt = table.insert()
                    mysql_session.execute(insert_stmt, batch)
                    total_inserted += len(batch)
                    
                    logger.debug(f"Batch {i//batch_size + 1}: inseriti {len(batch)} record")
                    
                except Exception as e:
                    errors += len(batch)
                    logger.error(f"ERRORE inserimento batch {i//batch_size + 1}: {str(e)}")
                    
                    # Fallback: prova inserimento singolo per questo batch
                    logger.warning(f"Tentativo inserimento singolo per batch {i//batch_size + 1}")
                    for j, record in enumerate(batch):
                        try:
                            insert_stmt = table.insert().values(**record)
                            mysql_session.execute(insert_stmt)
                            total_inserted += 1
                        except Exception as e2:
                            logger.error(f"ERRORE record {i+j+1}: {str(e2)}")
            
            if errors > 0:
                logger.warning(f"TOTALE ERRORI INSERIMENTO: {errors}")
            
            logger.info(f"Inserimenti completati: {total_inserted} record inseriti con successo")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Errore durante l'inserimento bulk per {self.table_name}: {str(e)}")
            raise

    def _prepare_insert_data(self, df: pl.DataFrame) -> List[Dict[str, Any]]:
        """Override con debug per preparazione dati inserimento."""
        if df.is_empty():
            logger.debug("DataFrame vuoto per prepare_insert_data")
            return []        
        
        logger.debug(f"Preparazione {len(df)} record per inserimento")
        
        # Rimuovi colonne con suffissi e colonne sistema
        cols_to_drop = [col for col in df.columns if 
                       col.endswith('_current') or 
                       col.endswith('_right') or 
                       col.startswith('_')]
        
        clean_df = df.drop(cols_to_drop) if cols_to_drop else df        
        
        # Rinomina colonne con suffisso _legacy
        rename_dict = {col: col.replace('_legacy', '') for col in clean_df.columns if col.endswith('_legacy')}
        if rename_dict:
            clean_df = clean_df.rename(rename_dict)
        
        # Converti in dizionari
        records = clean_df.to_dicts()        
        
        # Sanitizza per MySQL
        sanitized_records = [self._sanitize_for_mysql(record) for record in records]

        logger.debug(f"Sanitizzati {len(sanitized_records)} record")
        
        return sanitized_records

    def _save_discrepancies_to_file(self):
        """Salva le statistiche delle discrepanze nel file diff_nrildim.txt."""
        if not self.discrepancies:
            logger.info("Nessuna discrepanza statistica da salvare")
            return
        
        try:
            output_file = Path('./Notes/diff_nrildim.txt')
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            total_count = sum(self.discrepancies.values())
            unique_count = len(self.discrepancies)
            
            sorted_items = sorted(self.discrepancies.items(), 
                                key=lambda x: (x[0][0], x[0][1]))
            
            lines = []
            lines.append("=" * 150)
            lines.append(f"SESSION START: {timestamp} | PLANT: {self.plant}")
            lines.append(f"Total discrepancies: {total_count} (Unique: {unique_count})")
            lines.append("NOTA: Questi record sono stati inseriti con bPortingError=1")
            lines.append("=" * 150)
            
            for (error_type, ref_num, mold, workorder, article, press, reason), count in sorted_items:
                line = (f"[{timestamp}] {error_type} | "
                    f"RefNum={ref_num} | "
                    f"mold={mold} | "
                    f"workOrder={workorder} | "
                    f"article={article} | "
                    f"press={press} | "
                    f"reason={reason}")
                
                if count > 1:
                    line += f" [x{count}]"
                
                lines.append(line)
            
            lines.append("")
            
            with output_file.open('a', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
            
            logger.info(f"Statistiche salvate in {output_file}: {total_count} totali, {unique_count} uniche")
            
        except Exception as e:
            logger.error(f"Errore nel salvataggio statistiche: {str(e)}")

    def synchronize(self) -> Tuple[int, int, int]:
        try:
            logger.info(f"Inizio sincronizzazione per {self.table_name} - plant: {self.plant}")
            
            if self.is_multi_table_sync():
                if hasattr(self, '_execute_multi_table_sync'):
                    return self._execute_multi_table_sync()
                else:
                    raise NotImplementedError("Sottoclasse deve implementare _execute_multi_table_sync")
            
            with db_manager.get_sessions() as (mosys_session, mysql_session):
                legacy_data = self.fetch_legacy_data(mosys_session)
                current_data = self.fetch_current_data(mysql_session)                
                
                comparison_cols = self.get_comparison_columns()
                if not comparison_cols:
                     raise ValueError("Almeno una colonna di confronto Ă¨ richiesta per la sincronizzazione.")                                
                     
                merged_data = self.compare_datasets(legacy_data, current_data)
                to_insert, to_update, to_delete = self.identify_changes(merged_data)                
                
                inserted, updated, deleted = self.execute_bulk_operations(
                    to_insert, to_update, to_delete, mysql_session
                )

                functions.updateLogScheduler(self.table_name, self.plant)
                
                logger.info(f"Sincronizzazione completata per {self.table_name}: "
                        f"{inserted} inseriti, {updated} aggiornati, {deleted} eliminati")
                
                # Salva statistiche alla fine
                if hasattr(self, '_save_discrepancies_to_file'):
                    self._save_discrepancies_to_file()

                return inserted, updated, deleted
                    
        except Exception as e:
            logger.error(f"Errore durante la sincronizzazione di {self.table_name}: {str(e)}")
            raise


    # CORREZIONE 1: Modificare _load_lookup_caches per gestire mold con ultimi 9 caratteri
    def _load_lookup_caches(self):
        """Carica le cache per i lookup delle foreign key."""
        if all([self._workorder_cache, self._press_cache, self._mold_cache, self._article_cache]):
            return  
            
        try:
            with db_manager.get_sessions() as (_, mysql_session):
                
                # Cache per woStatic_tbl
                if not self._workorder_cache:
                    try:
                        workorder_query = text("""
                            SELECT DISTINCT TRIM(workOrder) as workOrder, idWorkOrder 
                            FROM woStatic_tbl 
                            WHERE plant = :plant 
                            AND workOrder IS NOT NULL 
                            AND TRIM(workOrder) != ''
                        """)
                        workorder_result = mysql_session.execute(workorder_query, {"plant": self.plant})
                        self._workorder_cache = {str(row.workOrder).strip(): int(row.idWorkOrder) 
                                                for row in workorder_result if row.workOrder}
                        
                    except Exception as e:
                        logger.warning(f"Errore caricamento cache workOrder: {e}")
                        self._workorder_cache = {}
                
                # Cache per presses_tbl
                if not self._press_cache:
                    try:
                        press_query = text("""
                            SELECT DISTINCT TRIM(press) as press, idPress 
                            FROM presses_tbl 
                            WHERE plant = :plant 
                            AND press IS NOT NULL 
                            AND TRIM(press) != ''
                        """)
                        press_result = mysql_session.execute(press_query, {"plant": self.plant})
                        self._press_cache = {str(row.press).strip(): int(row.idPress) 
                                        for row in press_result if row.press}
                        
                    except Exception as e:
                        logger.warning(f"Errore caricamento cache press: {e}")
                        self._press_cache = {}
                
                # Cache per moldStatic_tbl - USA ULTIMI 9 CARATTERI
                if not self._mold_cache:
                    try:
                        mold_query = text("""
                            SELECT DISTINCT 
                                RIGHT(TRIM(mold), 9) as mold_key,
                                idMold 
                            FROM moldStatic_tbl 
                            WHERE plant = :plant 
                            AND mold IS NOT NULL 
                            AND TRIM(mold) != ''
                        """)
                        mold_result = mysql_session.execute(mold_query, {"plant": self.plant})
                        self._mold_cache = {str(row.mold_key).strip(): int(row.idMold) 
                                        for row in mold_result if row.mold_key}
                        
                    except Exception as e:
                        logger.warning(f"Errore caricamento cache mold: {e}")
                        self._mold_cache = {}
                
                # Cache per articles_tbl
                if not self._article_cache:
                    try:
                        article_query = text("""
                            SELECT DISTINCT TRIM(article) as article, idArticle 
                            FROM articles_tbl 
                            WHERE plant = :plant 
                            AND article IS NOT NULL 
                            AND TRIM(article) != ''
                        """)
                        article_result = mysql_session.execute(article_query, {"plant": self.plant})
                        self._article_cache = {str(row.article).strip(): int(row.idArticle) 
                                            for row in article_result if row.article}
                        
                    except Exception as e:
                        logger.warning(f"Errore caricamento cache article: {e}")
                        self._article_cache = {}
                
        except Exception as e:
            logger.error(f"Errore generale nel caricamento delle cache: {str(e)}")
            self._workorder_cache = {}
            self._press_cache = {}
            self._mold_cache = {}
            self._article_cache = {}


    # CORREZIONE 2: Modificare transform_legacy_data per normalizzare mold
    def transform_legacy_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Trasforma i dati delle misure dal formato legacy con gestione errori."""
        if df.is_empty():
            return df
        
        self.debug_stats['raw_legacy_records'] = len(df)
        logger.debug(f"=== INIZIO TRASFORMAZIONE: {len(df)} record ===")
        
        try:
            measure_columns = [f'mis{i:02d}' for i in range(1, 21)]
            
            # STEP 1: Pulisci e normalizza i dati di base
            string_columns = ['article', 'mold', 'press', 'workOrder', 'operator', 'referenceNum', 
                            'measureDate', 'measureHour']
            
            for col in string_columns:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.String, strict=False)
                        .fill_null('')
                        .str.strip_chars()
                        .alias(col)
                    )
            
            # NORMALIZZA MOLD: prendi ultimi 9 caratteri
            if 'mold' in df.columns:
                df = df.with_columns(
                    pl.when(pl.col('mold').str.len_chars() > 9)
                    .then(pl.col('mold').str.slice(-9))
                    .otherwise(pl.col('mold'))
                    .alias('mold')
                )                
            
            # Gestione numPrint e numFigure
            df = df.with_columns([
                pl.col('numPrint')
                .cast(pl.Int64, strict=False)
                .fill_null(0)
                .alias('numPrint'),
                
                pl.col('numFigure')
                .cast(pl.Int64, strict=False)
                .fill_null(0)
                .alias('numFigure')
            ])
            
            self.debug_stats['after_basic_transform'] = len(df)
            
            # STEP 2: Processa le misure
            for col in measure_columns:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.String, strict=False)
                        .fill_null('')
                        .str.replace('S', '1')
                        .str.replace('N', '0') 
                        .str.replace('O', '0')
                        .str.strip_chars()
                        .map_elements(
                            lambda x: None if x in ('', ' ', 'null', 'NULL') else x, 
                            return_dtype=pl.String
                        )
                        .cast(pl.Float64, strict=False)
                        .alias(col)
                    )
            
            # STEP 3: Costruisci measureDateTime e traccia errori
            df = df.with_columns([
                pl.col('measureDate').str.zfill(8).alias('measureDate_norm'),
                pl.col('measureHour').str.zfill(6).alias('measureHour_norm')
            ])
            
            df = df.with_columns(
                pl.when(
                    (pl.col('measureDate_norm').str.len_chars() == 8) & 
                    (pl.col('measureHour_norm').str.len_chars() == 6) &
                    (pl.col('measureDate_norm').str.contains(r'^\d{8}$')) &
                    (pl.col('measureHour_norm').str.contains(r'^\d{6}$'))
                ).then(
                    pl.concat_str([
                        pl.col('measureDate_norm'),
                        pl.col('measureHour_norm')
                    ]).str.strptime(pl.Datetime("ms"), format='%Y%m%d%H%M%S', strict=False)
                ).otherwise(pl.lit(None, dtype=pl.Datetime("ms")))
                .alias('measureDateTime')
            )
            
            # STEP 4: Inizializza campi errore
            df = df.with_columns([
                pl.lit(False).alias('bPortingError'),
                pl.lit('').cast(pl.String).alias('portingErrorDesc')
            ])
            
            # Traccia errori datetime
            df = df.with_columns(
                pl.when(pl.col('measureDateTime').is_null())
                .then(pl.lit(True))
                .otherwise(pl.col('bPortingError'))
                .alias('bPortingError')
            )
            
            df = df.with_columns(
                pl.when(pl.col('measureDateTime').is_null() & (pl.col('portingErrorDesc') == ''))
                .then(pl.concat_str([
                    pl.lit('Invalid datetime format: date='),
                    pl.col('measureDate'),
                    pl.lit(', hour='),
                    pl.col('measureHour')
                ]))
                .when(pl.col('measureDateTime').is_null())
                .then(pl.concat_str([
                    pl.col('portingErrorDesc'),
                    pl.lit('; Invalid datetime format: date='),
                    pl.col('measureDate'),
                    pl.lit(', hour='),
                    pl.col('measureHour')
                ]))
                .otherwise(pl.col('portingErrorDesc'))
                .alias('portingErrorDesc')
            )
            
            # Traccia errori numPrint/numFigure
            df = df.with_columns(
                pl.when((pl.col('numPrint') == 0) | (pl.col('numFigure') == 0))
                .then(pl.lit(True))
                .otherwise(pl.col('bPortingError'))
                .alias('bPortingError')
            )
            
            df = df.with_columns(
                pl.when(
                    ((pl.col('numPrint') == 0) | (pl.col('numFigure') == 0)) & 
                    (pl.col('portingErrorDesc') == '')
                )
                .then(pl.concat_str([
                    pl.lit('Invalid numPrint='),
                    pl.col('numPrint').cast(pl.String),
                    pl.lit(' or numFigure='),
                    pl.col('numFigure').cast(pl.String)
                ]))
                .when((pl.col('numPrint') == 0) | (pl.col('numFigure') == 0))
                .then(pl.concat_str([
                    pl.col('portingErrorDesc'),
                    pl.lit('; Invalid numPrint='),
                    pl.col('numPrint').cast(pl.String),
                    pl.lit(' or numFigure='),
                    pl.col('numFigure').cast(pl.String)
                ]))
                .otherwise(pl.col('portingErrorDesc'))
                .alias('portingErrorDesc')
            )
            
            # Traccia errori referenceNum vuoto
            df = df.with_columns(
                pl.when(
                    pl.col('referenceNum').is_null() |
                    (pl.col('referenceNum').str.strip_chars() == '') |
                    (pl.col('referenceNum').str.strip_chars() == 'null') |
                    (pl.col('referenceNum').str.strip_chars() == 'NULL')
                )
                .then(pl.lit(True))
                .otherwise(pl.col('bPortingError'))
                .alias('bPortingError')
            )
            
            df = df.with_columns(
                pl.when(
                    (
                        pl.col('referenceNum').is_null() |
                        (pl.col('referenceNum').str.strip_chars() == '') |
                        (pl.col('referenceNum').str.strip_chars() == 'null') |
                        (pl.col('referenceNum').str.strip_chars() == 'NULL')
                    ) & (pl.col('portingErrorDesc') == '')
                )
                .then(pl.lit('Empty or null referenceNum'))
                .when(
                    pl.col('referenceNum').is_null() |
                    (pl.col('referenceNum').str.strip_chars() == '') |
                    (pl.col('referenceNum').str.strip_chars() == 'null') |
                    (pl.col('referenceNum').str.strip_chars() == 'NULL')
                )
                .then(pl.concat_str([
                    pl.col('portingErrorDesc'),
                    pl.lit('; Empty or null referenceNum')
                ]))
                .otherwise(pl.col('portingErrorDesc'))
                .alias('portingErrorDesc')
            )
            
            # Riempi referenceNum vuoti con placeholder
            df = df.with_columns(
                pl.when(
                    pl.col('referenceNum').is_null() |
                    (pl.col('referenceNum').str.strip_chars() == '') |
                    (pl.col('referenceNum').str.strip_chars() == 'null') |
                    (pl.col('referenceNum').str.strip_chars() == 'NULL')
                )
                .then(pl.concat_str([
                    pl.lit('EMPTY_'),
                    pl.col('measureDate'),
                    pl.lit('_'),
                    pl.col('measureHour')
                ]))
                .otherwise(pl.col('referenceNum'))
                .alias('referenceNum')
            )
            
            self.debug_stats['after_datetime_filter'] = len(df)
            
            # STEP 5: Calcola la misura
            measure_sum_expr = pl.lit(0.0)
            count_expr = pl.lit(0)
            
            for col in measure_columns:
                if col in df.columns:
                    measure_sum_expr = measure_sum_expr + pl.col(col).fill_null(0.0)
                    count_expr = count_expr + pl.col(col).is_not_null().cast(pl.Int32)
            
            df = df.with_columns([
                measure_sum_expr.alias('total_measures'),
                count_expr.alias('count_measures')
            ])
            
            df = df.with_columns(
                pl.when(pl.col('count_measures') > 0)
                .then((pl.col('total_measures') / pl.col('count_measures')) / 10000.0)
                .otherwise(0.0)
                .cast(pl.Float64)
                .alias('measure')
            )
            
            # STEP 6: Risolvi le foreign key
            df = self._resolve_foreign_keys(df)
            
            # STEP 7: Aggiungi colonne sistema
            df = df.with_columns([
                pl.lit(self.plant).cast(pl.String).alias('plant'),
                pl.lit(self.formatted_datetime).str.strptime(pl.Datetime("ms")).alias('lastModified')
            ])
            
            # STEP 8: Converti bPortingError in int
            df = df.with_columns(
                pl.col('bPortingError').cast(pl.Int8).alias('bPortingError')
            )
            
            # STEP 9: Seleziona colonne finali
            final_columns = [
                'idWorkOrder', 'idPress', 'idMold', 'idArticle',
                'measureDateTime', 'operator', 'referenceNum', 
                'numPrint', 'numFigure', 'measure', 
                'bPortingError', 'portingErrorDesc',
                'plant', 'lastModified'
            ]
            
            available_columns = [col for col in final_columns if col in df.columns]
            df = df.select(available_columns)
            
            # Statistiche finali
            error_stats = df.select([
                pl.col('bPortingError').sum().alias('with_errors'),
                (pl.col('bPortingError') == 0).sum().alias('without_errors')
            ]).row(0)
            
            self.debug_stats['records_with_errors'] = error_stats[0]
            self.debug_stats['records_without_errors'] = error_stats[1]
            self.debug_stats['final_records'] = len(df)
            
            logger.info(f"=== FINE TRASFORMAZIONE: {len(df)} record finali ===")
            logger.info(f"   - Con errori: {error_stats[0]}")
            logger.info(f"   - Senza errori: {error_stats[1]}")
            
            return df
            
        except Exception as e:
            logger.error(f"Errore durante la trasformazione dati misure: {str(e)}")
            raise


    # CORREZIONE 3: Aggiungere logging nella query Pervasive per verificare il numero originale
    def fetch_legacy_data(self, mosys_session: Session) -> pl.DataFrame:
        """Override del metodo per implementare la query ottimizzata a due fasi."""
        try:
            start_date, end_date = self.date_range
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            optimized_query = f"""
            SELECT NRILDIM.ARTICOLO as article, 
                NRILDIM.STAMPO as mold, 
                NRILDIM.PRESSA as press, 
                NRILDIM.COMMESSA as workOrder, 
                NRILDIM.OPERATORE as operator, 
                NRILDIM.DATA_RILEVAMENTO as measureDate, 
                NRILDIM.ORA_RILEVAMENTO as measureHour, 
                NRILDIM.NUMERO_RIFERIMENTO as referenceNum, 
                NRILDIM.NUMERO_STAMPATA as numPrint, 
                NRILDIM.NUMERO_FIGURA as numFigure, 
                NRILDIM.MIS01 as mis01, NRILDIM.MIS02 as mis02, NRILDIM.MIS03 as mis03, 
                NRILDIM.MIS04 as mis04, NRILDIM.MIS05 as mis05, NRILDIM.MIS06 as mis06, 
                NRILDIM.MIS07 as mis07, NRILDIM.MIS08 as mis08, NRILDIM.MIS09 as mis09, 
                NRILDIM.MIS10 as mis10, NRILDIM.MIS11 as mis11, NRILDIM.MIS12 as mis12, 
                NRILDIM.MIS13 as mis13, NRILDIM.MIS14 as mis14, NRILDIM.MIS15 as mis15, 
                NRILDIM.MIS16 as mis16, NRILDIM.MIS17 as mis17, NRILDIM.MIS18 as mis18, 
                NRILDIM.MIS19 as mis19, NRILDIM.MIS20 as mis20
            FROM NRILDIM
            WHERE NRILDIM.DATA_RILEVAMENTO >= '{start_date_str}'
            AND NRILDIM.DATA_RILEVAMENTO < '{end_date_str}'
            """
            
            result = mosys_session.execute(text(optimized_query))
            rows = result.fetchall()
            column_names = list(result.keys())

            if not rows:
                logger.warning(f"Nessun dato recuperato da NRILDIM per il range e commesse specificate")
                return pl.DataFrame()

            schema_overrides = self.get_legacy_column_types()
            if schema_overrides:
                schema_with_types = {col: schema_overrides.get(col, pl.Unknown) for col in column_names}
                df = pl.DataFrame(rows, schema=schema_with_types, orient="row")
            else:
                df = pl.DataFrame(rows, schema=column_names, orient="row")

            logger.debug(f"Recuperati {len(df)} record raw da NRILDIM")
            
            df = self.transform_legacy_data(df)
            self._validate_dataframe(df)

            logger.debug(f">>> RECORD FINALI DOPO TRASFORMAZIONE: {len(df)}")
            return df

        except Exception as e:
            logger.error(f"Errore durante il recupero dati legacy ottimizzato: {str(e)}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            raise

    def _resolve_foreign_keys(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Risolve le foreign key e traccia gli errori nei campi bPortingError e portingErrorDesc.
        I record con FK mancanti NON vengono piĂš rimossi ma segnalati.
        """
        if df.is_empty():
            return df.with_columns([
                pl.lit(None).cast(pl.Int64).alias('idWorkOrder'),
                pl.lit(None).cast(pl.Int64).alias('idPress'),
                pl.lit(None).cast(pl.Int64).alias('idMold'),
                pl.lit(None).cast(pl.Int64).alias('idArticle')
            ])
        
        try:
            logger.debug(f"   >>> Inizio risoluzione FK con {len(df)} record")
            self._load_lookup_caches()
            
            # Inizializza con NULL invece di 0
            df = df.with_columns([
                pl.lit(None).cast(pl.Int64).alias('idWorkOrder'),
                pl.lit(None).cast(pl.Int64).alias('idPress'),
                pl.lit(None).cast(pl.Int64).alias('idMold'),
                pl.lit(None).cast(pl.Int64).alias('idArticle')
            ])
            
            # Risolvi workOrder e traccia errori
            if self._workorder_cache:
                workorder_pl = pl.DataFrame(
                    list(self._workorder_cache.items()), 
                    schema=[('workOrder', pl.String), ('idWorkOrder_lookup', pl.Int64)], 
                    orient="row"
                )
                df = df.join(workorder_pl, on='workOrder', how='left')
                
                # Conta match/non-match
                wo_stats = df.select([
                    (pl.col('workOrder') != '').sum().alias('non_empty'),
                    ((pl.col('workOrder') != '') & pl.col('idWorkOrder_lookup').is_not_null()).sum().alias('matched'),
                    ((pl.col('workOrder') != '') & pl.col('idWorkOrder_lookup').is_null()).sum().alias('not_found')
                ]).row(0)
                print(f"   >>> WorkOrder: {wo_stats[0]} non vuoti, {wo_stats[1]} trovati, {wo_stats[2]} non trovati")
                
                # Traccia errore se workOrder non vuoto ma non trovato
                df = df.with_columns(
                    pl.when(
                        (pl.col('workOrder') != '') & 
                        pl.col('idWorkOrder_lookup').is_null()
                    )
                    .then(pl.lit(True))
                    .otherwise(pl.col('bPortingError'))
                    .alias('bPortingError')
                )
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('workOrder') != '') & 
                        pl.col('idWorkOrder_lookup').is_null() &
                        (pl.col('portingErrorDesc') == '')
                    )
                    .then(pl.concat_str([
                        pl.lit('WorkOrder not found: '),
                        pl.col('workOrder')
                    ]))
                    .when(
                        (pl.col('workOrder') != '') & 
                        pl.col('idWorkOrder_lookup').is_null()
                    )
                    .then(pl.concat_str([
                        pl.col('portingErrorDesc'),
                        pl.lit('; WorkOrder not found: '),
                        pl.col('workOrder')
                    ]))
                    .otherwise(pl.col('portingErrorDesc'))
                    .alias('portingErrorDesc')
                )
                
                df = df.with_columns(
                    pl.coalesce([pl.col('idWorkOrder_lookup'), pl.col('idWorkOrder')])
                    .alias('idWorkOrder')
                ).drop('idWorkOrder_lookup')
            
            logger.debug(f"   >>> Dopo risoluzione workOrder: {len(df)} record")
            
            # Risolvi press e traccia errori
            if self._press_cache:
                press_pl = pl.DataFrame(
                    list(self._press_cache.items()),
                    schema=[('press', pl.String), ('idPress_lookup', pl.Int64)],
                    orient="row"  
                )
                df = df.join(press_pl, on='press', how='left')
                
                press_stats = df.select([
                    (pl.col('press') != '').sum().alias('non_empty'),
                    ((pl.col('press') != '') & pl.col('idPress_lookup').is_not_null()).sum().alias('matched'),
                    ((pl.col('press') != '') & pl.col('idPress_lookup').is_null()).sum().alias('not_found')
                ]).row(0)
                print(f"   >>> Press: {press_stats[0]} non vuoti, {press_stats[1]} trovati, {press_stats[2]} non trovati")
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('press') != '') & 
                        pl.col('idPress_lookup').is_null()
                    )
                    .then(pl.lit(True))
                    .otherwise(pl.col('bPortingError'))
                    .alias('bPortingError')
                )
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('press') != '') & 
                        pl.col('idPress_lookup').is_null() &
                        (pl.col('portingErrorDesc') == '')
                    )
                    .then(pl.concat_str([
                        pl.lit('Press not found: '),
                        pl.col('press')
                    ]))
                    .when(
                        (pl.col('press') != '') & 
                        pl.col('idPress_lookup').is_null()
                    )
                    .then(pl.concat_str([
                        pl.col('portingErrorDesc'),
                        pl.lit('; Press not found: '),
                        pl.col('press')
                    ]))
                    .otherwise(pl.col('portingErrorDesc'))
                    .alias('portingErrorDesc')
                )
                
                df = df.with_columns(
                    pl.coalesce([pl.col('idPress_lookup'), pl.col('idPress')])
                    .alias('idPress')
                ).drop('idPress_lookup')
            
            logger.debug(f"   >>> Dopo risoluzione press: {len(df)} record")
            
            # Risolvi mold e traccia errori
            if self._mold_cache:
                mold_pl = pl.DataFrame(
                    list(self._mold_cache.items()),
                    schema=[('mold', pl.String), ('idMold_lookup', pl.Int64)],
                    orient="row"  
                )
                df = df.join(mold_pl, on='mold', how='left')
                
                mold_stats = df.select([
                    (pl.col('mold') != '').sum().alias('non_empty'),
                    ((pl.col('mold') != '') & pl.col('idMold_lookup').is_not_null()).sum().alias('matched'),
                    ((pl.col('mold') != '') & pl.col('idMold_lookup').is_null()).sum().alias('not_found')
                ]).row(0)
                logger.debug(f"   >>> Mold: {mold_stats[0]} non vuoti, {mold_stats[1]} trovati, {mold_stats[2]} non trovati")
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('mold') != '') & 
                        pl.col('idMold_lookup').is_null()
                    )
                    .then(pl.lit(True))
                    .otherwise(pl.col('bPortingError'))
                    .alias('bPortingError')
                )
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('mold') != '') & 
                        pl.col('idMold_lookup').is_null() &
                        (pl.col('portingErrorDesc') == '')
                    )
                    .then(pl.concat_str([
                        pl.lit('Mold not found: '),
                        pl.col('mold')
                    ]))
                    .when(
                        (pl.col('mold') != '') & 
                        pl.col('idMold_lookup').is_null()
                    )
                    .then(pl.concat_str([
                        pl.col('portingErrorDesc'),
                        pl.lit('; Mold not found: '),
                        pl.col('mold')
                    ]))
                    .otherwise(pl.col('portingErrorDesc'))
                    .alias('portingErrorDesc')
                )
                
                df = df.with_columns(
                    pl.coalesce([pl.col('idMold_lookup'), pl.col('idMold')])
                    .alias('idMold')
                ).drop('idMold_lookup')
            
            logger.debug(f"   >>> Dopo risoluzione mold: {len(df)} record")
            
            # Risolvi article e traccia errori
            if self._article_cache:
                article_pl = pl.DataFrame(
                    list(self._article_cache.items()),
                    schema=[('article', pl.String), ('idArticle_lookup', pl.Int64)],
                    orient="row"
                )
                df = df.join(article_pl, on='article', how='left')
                
                article_stats = df.select([
                    (pl.col('article') != '').sum().alias('non_empty'),
                    ((pl.col('article') != '') & pl.col('idArticle_lookup').is_not_null()).sum().alias('matched'),
                    ((pl.col('article') != '') & pl.col('idArticle_lookup').is_null()).sum().alias('not_found')
                ]).row(0)
                logger.debug(f"   >>> Article: {article_stats[0]} non vuoti, {article_stats[1]} trovati, {article_stats[2]} non trovati")
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('article') != '') & 
                        pl.col('idArticle_lookup').is_null()
                    )
                    .then(pl.lit(True))
                    .otherwise(pl.col('bPortingError'))
                    .alias('bPortingError')
                )
                
                df = df.with_columns(
                    pl.when(
                        (pl.col('article') != '') & 
                        pl.col('idArticle_lookup').is_null() &
                        (pl.col('portingErrorDesc') == '')
                    )
                    .then(pl.concat_str([
                        pl.lit('Article not found: '),
                        pl.col('article')
                    ]))
                    .when(
                        (pl.col('article') != '') & 
                        pl.col('idArticle_lookup').is_null()
                    )
                    .then(pl.concat_str([
                        pl.col('portingErrorDesc'),
                        pl.lit('; Article not found: '),
                        pl.col('article')
                    ]))
                    .otherwise(pl.col('portingErrorDesc'))
                    .alias('portingErrorDesc')
                )
                
                df = df.with_columns(
                    pl.coalesce([pl.col('idArticle_lookup'), pl.col('idArticle')])
                    .alias('idArticle')
                ).drop('idArticle_lookup')
            
            logger.debug(f"   >>> Dopo risoluzione article: {len(df)} record")
            logger.debug(f"   >>> FK resolution completata: {len(df)} record processati")
            return df
            
        except Exception as e:
            logger.error(f"Errore nella risoluzione delle foreign key: {str(e)}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return df.with_columns([
                pl.lit(None).cast(pl.Int64).alias('idWorkOrder'),
                pl.lit(None).cast(pl.Int64).alias('idPress'),
                pl.lit(None).cast(pl.Int64).alias('idMold'),
                pl.lit(None).cast(pl.Int64).alias('idArticle')
            ])
        
        






def updateNrildim(plant: str = None) -> tuple:
    """Funzione per l'aggiornamento delle misure dimensionali con gestione errori."""
    with timer.Timer("nRildim sync con gestione errori"):
        if plant is None:            
            plant = PLANT
        
        try:
            manager = nRildimSyncManager(plant)
            result = manager.synchronize()           
                        
            logger.debug(f"Statistiche debug per plant {plant}:")
            for key, value in manager.debug_stats.items():
                print(f"  {key}: {value}")
                
            return result
            
        except Exception as e:
            logger.error(f"Errore durante l'aggiornamento misure per plant {plant}: {str(e)}")
            raise

def main() -> None:
    """Funzione principale del programma."""
    logger.debug("Avvio sincronizzazione nRildim con gestione errori")
    
    try:
        inserted, updated, deleted = updateNrildim(PLANT)
        
        result_data = {
            "status": "completed",
            "message": "Sincronizzazione completata con successo",
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted
        }
        
        logger.debug("Risultato finale JSON:")
        logger.debug(json.dumps(result_data, indent=2))
        
    except KeyboardInterrupt:
        result_data = {
            "status": "failed",
            "message": "Processo interrotto dall'utente",
            "inserted": 0, "updated": 0, "deleted": 0
        }
        logger.debug(json.dumps(result_data))
        sys.exit(1)
        
    except Exception as e:
        result_data = {
            "status": "failed",
            "message": f"Errore critico: {str(e)}",
            "inserted": 0, "updated": 0, "deleted": 0
        }
        print(json.dumps(result_data))
        sys.exit(1)

def main_recursive() -> None:
    """Funzione principale con logica progressiva ottimizzata."""
    try:
        manager = nRildimSyncManager(PLANT)
        current_date_range = manager.date_range
        today = datetime.now()
        
        print(f"=== SINCRONIZZAZIONE PROGRESSIVA NRILDIM - PLANT: {PLANT} ===")
        print(f"Data di partenza: {current_date_range[0].strftime('%Y-%m-%d')}")
        print(f"Data finale obiettivo: {today.strftime('%Y-%m-%d')}")
        print("=" * 60)
        
        iteration = 1
        total_inserted = 0
        total_updated = 0
        total_deleted = 0
        
        while current_date_range[0] < today:
            print(f"\n--- ITERAZIONE {iteration} ---")
            print(f"Periodo: {current_date_range[0].strftime('%Y-%m-%d')} -> {current_date_range[1].strftime('%Y-%m-%d')}")
            
            try:
                manager.update_date_range(current_date_range[0], current_date_range[1])
                inserted, updated, deleted = manager.synchronize()
        
                result_data = {
                    "status": "completed",
                    "message": "Iterazione completata con successo",
                    "inserted": inserted,
                    "updated": updated,
                    "deleted": deleted
                }
                
                logger.debug(json.dumps(result_data, indent=2))
                
                total_inserted += inserted
                total_updated += updated
                total_deleted += deleted
                
                next_start = current_date_range[1]
                next_end = min(next_start + timedelta(days=30), today)
                current_date_range = (next_start, next_end)
                iteration += 1
                
                import time
                time.sleep(3)
                
            except Exception as e:
                logger.debug(f"Errore nell'iterazione {iteration}: {str(e)}")
                next_start = current_date_range[1]
                next_end = min(next_start + timedelta(days=30), today)
                current_date_range = (next_start, next_end)
                iteration += 1
        
        print("\n" + "=" * 60)
        print("SINCRONIZZAZIONE COMPLETATA")
        print(f"Totale iterazioni: {iteration-1}")
        print(f"Risultati: inserted={total_inserted}, updated={total_updated}, deleted={total_deleted}")
        print("=" * 60)
        
    except KeyboardInterrupt:
        logger.error("Processo interrotto dall'utente")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Errore critico: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--recursive':
        main_recursive()
    else:
        main()