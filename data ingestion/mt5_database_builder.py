import os
import sqlite3
import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime, timedelta
import json
import time
from pathlib import Path
import logging
from typing import List, Dict, Optional, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


class MT5DatabaseBuilder:
    def __init__(self, db_path: str = "mt5_historical_data.db", config_file: str = "mt5_config.json"):
        self.db_path = db_path
        self.config_file = config_file
        self.connection = None
        self.mt5_connected = False

        # Configurazione logging
        self.setup_logging()

        # Abilita debug se necessario
        if os.getenv('MT5_DEBUG', '').lower() in ['1', 'true', 'yes']:
            logging.getLogger().setLevel(logging.DEBUG)

        # Timeframes supportati
        self.timeframes = {
            'M1': mt5.TIMEFRAME_M1,
            'M5': mt5.TIMEFRAME_M5,
            'M15': mt5.TIMEFRAME_M15,
            'M30': mt5.TIMEFRAME_M30,
            'H1': mt5.TIMEFRAME_H1,
            'H4': mt5.TIMEFRAME_H4,
            'D1': mt5.TIMEFRAME_D1,
            'W1': mt5.TIMEFRAME_W1,
            'MN1': mt5.TIMEFRAME_MN1
        }

        # Statistiche
        self.stats = {
            'symbols_processed': 0,
            'total_bars_downloaded': 0,
            'errors': 0,
            'start_time': None,
            'symbols_completed': [],
            'symbols_failed': []
        }

    def setup_logging(self):
        """Configura il sistema di logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('mt5_database_builder.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_config(self) -> Dict:
        """Carica la configurazione da file JSON"""
        default_config = {
            "mt5_executable_path": "",
            "symbols_to_download": [],
            "timeframes": ["M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"],
            "max_bars_per_request": 50000,
            "batch_size": 10,
            "include_indices": True,
            "include_forex": True,
            "include_commodities": True,
            "include_crypto": True,
            "auto_detect_symbols": True,
            "max_workers": 4
        }

        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # Merge con configurazione default
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            except Exception as e:
                self.logger.error(f"Errore nel caricamento config: {e}")
                return default_config
        else:
            # Crea file di configurazione default
            self.save_config(default_config)
            return default_config

    def save_config(self, config: Dict):
        """Salva la configurazione su file JSON"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Configurazione salvata in {self.config_file}")
        except Exception as e:
            self.logger.error(f"Errore nel salvataggio config: {e}")

    def connect_to_mt5(self, executable_path: str = None) -> bool:
        """Connette a MetaTrader 5"""
        try:
            if executable_path and os.path.exists(executable_path):
                success = mt5.initialize(path=executable_path)
            else:
                success = mt5.initialize()

            if not success:
                self.logger.error(f"Impossibile connettersi a MT5: {mt5.last_error()}")
                return False

            # Informazioni di connessione
            terminal_info = mt5.terminal_info()
            account_info = mt5.account_info()

            self.logger.info("🎯 Connesso a MetaTrader 5!")
            self.logger.info(f"📊 Terminale: {terminal_info.name}")
            self.logger.info(f"🏢 Server: {account_info.server}")
            self.logger.info(f"💼 Account: {account_info.login}")

            self.mt5_connected = True
            return True

        except Exception as e:
            self.logger.error(f"Errore nella connessione MT5: {e}")
            return False

    def setup_database(self):
        """Inizializza il database SQLite"""
        try:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = self.connection.cursor()

            # Tabella per i dati OHLCV
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    time INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    tick_volume INTEGER,
                    spread INTEGER,
                    real_volume INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timeframe, time)
                )
            ''')

            # Tabella per metadata dei simboli
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS symbol_info (
                    symbol TEXT PRIMARY KEY,
                    description TEXT,
                    currency_base TEXT,
                    currency_profit TEXT,
                    currency_margin TEXT,
                    digits INTEGER,
                    point REAL,
                    trade_mode INTEGER,
                    trade_execution INTEGER,
                    swap_mode INTEGER,
                    category TEXT,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Tabella per statistiche download
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS download_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    bars_count INTEGER NOT NULL,
                    first_date TEXT,
                    last_date TEXT,
                    download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN DEFAULT TRUE
                )
            ''')

            # Indici per performance
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_symbol_timeframe_time ON price_data(symbol, timeframe, time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_time ON price_data(symbol, time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timeframe_time ON price_data(timeframe, time)')

            self.connection.commit()
            self.logger.info(f"📊 Database inizializzato: {self.db_path}")

        except Exception as e:
            self.logger.error(f"Errore nell'inizializzazione database: {e}")
            raise

    def get_available_symbols(self) -> List[str]:
        """Ottiene tutti i simboli disponibili da MT5"""
        if not self.mt5_connected:
            self.logger.error("MT5 non connesso!")
            return []

        try:
            symbols = mt5.symbols_get()
            if not symbols:
                self.logger.warning("Nessun simbolo trovato")
                return []

            symbol_list = [s.name for s in symbols]
            self.logger.info(f"🔍 Trovati {len(symbol_list)} simboli disponibili")

            # Salva info simboli nel database
            self.save_symbols_info(symbols)

            return symbol_list

        except Exception as e:
            self.logger.error(f"Errore nel recupero simboli: {e}")
            return []

    def save_symbols_info(self, symbols):
        """Salva informazioni sui simboli nel database"""
        try:
            cursor = self.connection.cursor()

            for symbol in symbols:
                # Determina categoria
                category = self.classify_symbol(symbol.name)

                cursor.execute('''
                    INSERT OR REPLACE INTO symbol_info 
                    (symbol, description, currency_base, currency_profit, currency_margin, 
                     digits, point, trade_mode, trade_execution, swap_mode, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol.name,
                    getattr(symbol, 'description', ''),
                    getattr(symbol, 'currency_base', ''),
                    getattr(symbol, 'currency_profit', ''),
                    getattr(symbol, 'currency_margin', ''),
                    getattr(symbol, 'digits', 0),
                    getattr(symbol, 'point', 0.0),
                    getattr(symbol, 'trade_mode', 0),
                    getattr(symbol, 'trade_execution', 0),
                    getattr(symbol, 'swap_mode', 0),
                    category
                ))

            self.connection.commit()
            self.logger.info(f"💾 Salvate informazioni per {len(symbols)} simboli")

        except Exception as e:
            self.logger.error(f"Errore nel salvataggio info simboli: {e}")

    def classify_symbol(self, symbol: str) -> str:
        """Classifica il simbolo per categoria"""
        symbol_upper = symbol.upper()

        # Indici
        index_patterns = ['US30', 'NAS100', 'SPX500', 'UK100', 'GER30', 'FRA40', 'JPN225', 'AUS200', 'USTEC', 'VIX']
        if any(pattern in symbol_upper for pattern in index_patterns):
            return 'INDEX'

        # Forex
        forex_patterns = ['EUR', 'USD', 'GBP', 'JPY', 'CHF', 'AUD', 'CAD', 'NZD']
        if len(symbol) == 6 and any(pattern in symbol_upper for pattern in forex_patterns):
            return 'FOREX'

        # Commodities
        commodity_patterns = ['GOLD', 'SILVER', 'OIL', 'BRENT', 'XAU', 'XAG', 'USO']
        if any(pattern in symbol_upper for pattern in commodity_patterns):
            return 'COMMODITY'

        # Crypto
        crypto_patterns = ['BTC', 'ETH', 'LTC', 'XRP', 'ADA', 'DOT']
        if any(pattern in symbol_upper for pattern in crypto_patterns):
            return 'CRYPTO'

        return 'OTHER'

    def download_symbol_data(self, symbol: str, timeframes: List[str], max_bars: int = 50000) -> Dict:
        """Scarica dati storici per un simbolo specifico"""
        results = {'symbol': symbol, 'timeframes': {}, 'total_bars': 0, 'success': True}

        try:
            # Verifica che il simbolo sia selezionabile
            if not mt5.symbol_select(symbol, True):
                self.logger.warning(f"Impossibile selezionare il simbolo {symbol}")
                results['success'] = False
                results['error'] = f"Simbolo {symbol} non selezionabile"
                return results

            for tf_name in timeframes:
                if tf_name not in self.timeframes:
                    self.logger.warning(f"Timeframe {tf_name} non supportato")
                    continue

                tf_value = self.timeframes[tf_name]

                try:
                    # Prova a scaricare i dati
                    rates = mt5.copy_rates_from_pos(symbol, tf_value, 0, max_bars)

                    if rates is not None and len(rates) > 0:
                        # Converti in DataFrame
                        df = pd.DataFrame(rates)

                        # Debug: mostra info sul DataFrame
                        self.logger.debug(f"{symbol} {tf_name}: {len(df)} righe, colonne: {df.columns.tolist()}")

                        # Conversione sicura del timestamp
                        if 'time' in df.columns:
                            df['time'] = pd.to_datetime(df['time'], unit='s', errors='coerce')
                            # Rimuovi righe con timestamp invalidi
                            df = df.dropna(subset=['time'])

                        if len(df) == 0:
                            self.logger.warning(f"{symbol} {tf_name}: Nessun dato valido dopo la pulizia")
                            results['timeframes'][tf_name] = {
                                'bars_count': 0,
                                'bars_saved': 0,
                                'success': False,
                                'error': 'Nessun dato valido dopo la pulizia'
                            }
                            continue

                        # Salva nel database
                        bars_saved = self.save_price_data(symbol, tf_name, df)

                        results['timeframes'][tf_name] = {
                            'bars_count': len(df),
                            'bars_saved': bars_saved,
                            'first_date': df['time'].iloc[0].isoformat() if len(df) > 0 else None,
                            'last_date': df['time'].iloc[-1].isoformat() if len(df) > 0 else None,
                            'success': bars_saved > 0
                        }

                        results['total_bars'] += bars_saved
                        self.stats['total_bars_downloaded'] += bars_saved

                        if bars_saved > 0:
                            self.logger.info(f"✅ {symbol} {tf_name}: {bars_saved:,} barre salvate")
                        else:
                            self.logger.warning(f"⚠️  {symbol} {tf_name}: 0 barre salvate (possibili errori nei dati)")

                    else:
                        error_info = mt5.last_error()
                        results['timeframes'][tf_name] = {
                            'bars_count': 0,
                            'bars_saved': 0,
                            'success': False,
                            'error': f'Nessun dato disponibile - MT5 Error: {error_info}'
                        }
                        self.logger.warning(f"⚠️  {symbol} {tf_name}: Nessun dato disponibile - {error_info}")

                except Exception as e:
                    results['timeframes'][tf_name] = {
                        'bars_count': 0,
                        'bars_saved': 0,
                        'success': False,
                        'error': str(e)
                    }
                    self.logger.error(f"❌ {symbol} {tf_name}: {e}")
                    self.stats['errors'] += 1

                # Pausa breve per non sovraccaricare MT5
                time.sleep(0.1)

            return results

        except Exception as e:
            self.logger.error(f"Errore nel download di {symbol}: {e}")
            results['success'] = False
            results['error'] = str(e)
            self.stats['errors'] += 1
            return results

    def save_price_data(self, symbol: str, timeframe: str, df: pd.DataFrame) -> int:
        """Salva i dati di prezzo nel database"""
        try:
            cursor = self.connection.cursor()

            # Debug: mostra le colonne disponibili e i primi dati
            self.logger.debug(f"=== DEBUG {symbol} {timeframe} ===")
            self.logger.debug(f"Colonne DataFrame: {df.columns.tolist()}")
            self.logger.debug(f"Dtypes: {df.dtypes.to_dict()}")
            self.logger.debug(f"Forma DataFrame: {df.shape}")

            if len(df) > 0:
                self.logger.debug(f"Primo record raw: {df.iloc[0].to_dict()}")

            # Prepara i dati per l'inserimento con controlli più rigorosi
            data_to_insert = []
            problematic_rows = 0

            for idx, row in df.iterrows():
                try:
                    # Debug per la prima riga
                    if idx == 0:
                        self.logger.debug(f"Processing first row: {row.to_dict()}")

                    # Conversioni sicure con gestione errori
                    if 'time' not in row or pd.isna(row['time']):
                        self.logger.debug(f"Riga {idx}: timestamp mancante o invalido")
                        problematic_rows += 1
                        continue

                    time_val = int(row['time'].timestamp()) if hasattr(row['time'], 'timestamp') else int(row['time'])

                    # Verifica valori OHLC
                    required_fields = ['open', 'high', 'low', 'close']
                    ohlc_values = {}

                    for field in required_fields:
                        if field not in row:
                            self.logger.debug(f"Riga {idx}: campo {field} mancante")
                            problematic_rows += 1
                            break

                        value = row[field]
                        if pd.isna(value) or value <= 0:
                            self.logger.debug(f"Riga {idx}: {field} = {value} (invalido)")
                            problematic_rows += 1
                            break

                        try:
                            ohlc_values[field] = float(value)
                        except (ValueError, TypeError) as e:
                            self.logger.debug(f"Riga {idx}: Errore conversione {field} = {value}: {e}")
                            problematic_rows += 1
                            break
                    else:
                        # Se tutti i campi OHLC sono OK, procedi

                        # Gestione volume con fallback sicuro
                        volume_val = 0
                        tick_volume_val = 0

                        if 'tick_volume' in row and not pd.isna(row['tick_volume']):
                            try:
                                volume_val = int(float(row['tick_volume']))
                                tick_volume_val = volume_val
                            except (ValueError, TypeError):
                                volume_val = 0
                                tick_volume_val = 0

                        # Gestione spread
                        spread_val = 0
                        if 'spread' in row and not pd.isna(row['spread']):
                            try:
                                spread_val = int(float(row['spread']))
                            except (ValueError, TypeError):
                                spread_val = 0

                        # Gestione real_volume
                        real_volume_val = 0
                        if 'real_volume' in row and not pd.isna(row['real_volume']):
                            try:
                                real_volume_val = int(float(row['real_volume']))
                            except (ValueError, TypeError):
                                real_volume_val = 0

                        record = (
                            str(symbol)[:50],  # Limita lunghezza
                            str(timeframe)[:10],  # Limita lunghezza
                            time_val,
                            ohlc_values['open'],
                            ohlc_values['high'],
                            ohlc_values['low'],
                            ohlc_values['close'],
                            volume_val,
                            tick_volume_val,
                            spread_val,
                            real_volume_val
                        )

                        # Debug per il primo record
                        if len(data_to_insert) == 0:
                            self.logger.debug(f"Primo record preparato: {record}")
                            # Verifica tipi
                            types_check = [f"{type(v).__name__}: {v}" for v in record]
                            self.logger.debug(f"Tipi: {types_check}")

                        data_to_insert.append(record)

                except Exception as row_error:
                    self.logger.debug(f"Errore nella riga {idx} di {symbol} {timeframe}: {row_error}")
                    problematic_rows += 1
                    continue

            self.logger.debug(f"Righe preparate: {len(data_to_insert)}, Problematiche: {problematic_rows}")

            if not data_to_insert:
                self.logger.warning(f"Nessun dato valido da inserire per {symbol} {timeframe}")
                return 0

            # Test di inserimento singolo prima del batch
            if len(data_to_insert) > 0:
                try:
                    self.logger.debug("Test inserimento singolo record...")
                    cursor.execute('''
                        INSERT OR IGNORE INTO price_data 
                        (symbol, timeframe, time, open, high, low, close, volume, tick_volume, spread, real_volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', data_to_insert[0])

                    test_rows = cursor.rowcount
                    self.logger.debug(f"Test inserimento OK: {test_rows} riga inserita")

                except sqlite3.Error as test_error:
                    self.logger.error(f"ERRORE TEST INSERIMENTO {symbol} {timeframe}: {test_error}")
                    self.logger.error(f"Record problematico: {data_to_insert[0]}")
                    return 0

            # Inserimento batch con gestione duplicati
            try:
                self.logger.debug(f"Inserimento batch di {len(data_to_insert)} record...")
                cursor.executemany('''
                    INSERT OR IGNORE INTO price_data 
                    (symbol, timeframe, time, open, high, low, close, volume, tick_volume, spread, real_volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert[1:])  # Skip il primo già inserito nel test

                rows_inserted = cursor.rowcount + (1 if len(data_to_insert) > 0 else 0)  # +1 per il test record
                self.connection.commit()

                self.logger.debug(f"Batch inserimento completato: {rows_inserted} righe")

            except sqlite3.Error as db_error:
                self.logger.error(f"ERRORE BATCH INSERIMENTO {symbol} {timeframe}: {db_error}")
                self.logger.error(f"Errore tipo: {type(db_error).__name__}")
                # Prova inserimento record per record per identificare il problema
                self.logger.debug("Tentativo inserimento record per record...")
                rows_inserted = 0
                for i, record in enumerate(data_to_insert[1:], 1):  # Skip first
                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO price_data 
                            (symbol, timeframe, time, open, high, low, close, volume, tick_volume, spread, real_volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', record)
                        rows_inserted += cursor.rowcount
                    except sqlite3.Error as single_error:
                        self.logger.error(f"Errore record {i}: {single_error}")
                        self.logger.error(f"Record: {record}")
                        break

                if rows_inserted > 0:
                    self.connection.commit()
                    rows_inserted += 1  # Add test record
                else:
                    return 0

            # Salva statistiche solo se ci sono dati
            if len(df) > 0 and rows_inserted > 0:
                try:
                    cursor.execute('''
                        INSERT INTO download_stats 
                        (symbol, timeframe, bars_count, first_date, last_date, success)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        str(symbol),
                        str(timeframe),
                        len(df),
                        df['time'].iloc[0].isoformat(),
                        df['time'].iloc[-1].isoformat(),
                        True
                    ))
                    self.connection.commit()
                except sqlite3.Error as stats_error:
                    self.logger.warning(f"Errore nel salvataggio statistiche per {symbol}: {stats_error}")

            return rows_inserted

        except Exception as e:
            self.logger.error(f"Errore generale nel salvataggio dati {symbol} {timeframe}: {e}")
            self.logger.error(f"Tipo errore: {type(e).__name__}")
            import traceback
            self.logger.debug(f"Traceback completo: {traceback.format_exc()}")
            return 0

    def filter_symbols_by_category(self, symbols: List[str], config: Dict) -> List[str]:
        """Filtra i simboli in base alle categorie selezionate"""
        filtered_symbols = []

        for symbol in symbols:
            category = self.classify_symbol(symbol)

            if (category == 'INDEX' and config.get('include_indices', True)) or \
                    (category == 'FOREX' and config.get('include_forex', True)) or \
                    (category == 'COMMODITY' and config.get('include_commodities', True)) or \
                    (category == 'CRYPTO' and config.get('include_crypto', True)) or \
                    (category == 'OTHER'):
                filtered_symbols.append(symbol)

        return filtered_symbols

    def build_database(self):
        """Costruisce il database completo dei dati storici"""
        self.logger.info("🚀 Inizio costruzione database dati storici MT5")
        self.stats['start_time'] = datetime.now()

        try:
            # Carica configurazione
            config = self.load_config()
            self.logger.info(f"📋 Configurazione caricata: {config}")

            # Connetti a MT5
            if not self.connect_to_mt5(config.get('mt5_executable_path')):
                return False

            # Inizializza database
            self.setup_database()

            # Ottieni simboli disponibili
            if config.get('auto_detect_symbols', True):
                all_symbols = self.get_available_symbols()
                symbols_to_process = self.filter_symbols_by_category(all_symbols, config)
            else:
                symbols_to_process = config.get('symbols_to_download', [])

            if not symbols_to_process:
                self.logger.error("❌ Nessun simbolo da processare!")
                return False

            self.logger.info(f"📊 Simboli da processare: {len(symbols_to_process)}")

            # Timeframes da scaricare
            timeframes = config.get('timeframes', ['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'])
            max_bars = config.get('max_bars_per_request', 50000)
            max_workers = config.get('max_workers', 4)

            # Download parallelo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_symbol = {
                    executor.submit(self.download_symbol_data, symbol, timeframes, max_bars): symbol
                    for symbol in symbols_to_process
                }

                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result()
                        self.stats['symbols_processed'] += 1

                        if result['success']:
                            self.stats['symbols_completed'].append(symbol)
                            self.logger.info(f"✅ Completato {symbol}: {result['total_bars']:,} barre totali")
                        else:
                            self.stats['symbols_failed'].append(symbol)
                            self.logger.error(f"❌ Fallito {symbol}")

                        # Progress update
                        progress = (self.stats['symbols_processed'] / len(symbols_to_process)) * 100
                        self.logger.info(
                            f"📈 Progresso: {progress:.1f}% ({self.stats['symbols_processed']}/{len(symbols_to_process)})")

                    except Exception as e:
                        self.stats['symbols_failed'].append(symbol)
                        self.logger.error(f"❌ Errore per {symbol}: {e}")

            # Statistiche finali
            self.print_final_stats()
            return True

        except Exception as e:
            self.logger.error(f"Errore generale: {e}")
            return False
        finally:
            self.cleanup()

    def print_final_stats(self):
        """Stampa le statistiche finali"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']

        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 STATISTICHE FINALI")
        self.logger.info("=" * 60)
        self.logger.info(f"⏱️  Durata totale: {duration}")
        self.logger.info(f"📈 Simboli processati: {self.stats['symbols_processed']}")
        self.logger.info(f"✅ Simboli completati: {len(self.stats['symbols_completed'])}")
        self.logger.info(f"❌ Simboli falliti: {len(self.stats['symbols_failed'])}")
        self.logger.info(f"📊 Barre totali scaricate: {self.stats['total_bars_downloaded']:,}")
        self.logger.info(f"⚠️  Errori totali: {self.stats['errors']}")

        if self.stats['symbols_failed']:
            self.logger.info(f"\n❌ Simboli falliti: {', '.join(self.stats['symbols_failed'])}")

        # Statistiche database
        try:
            cursor = self.connection.cursor()

            cursor.execute("SELECT COUNT(*) FROM price_data")
            total_records = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM price_data")
            unique_symbols = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT timeframe) FROM price_data")
            unique_timeframes = cursor.fetchone()[0]

            self.logger.info(f"\n💾 DATABASE:")
            self.logger.info(f"   📊 Record totali: {total_records:,}")
            self.logger.info(f"   💱 Simboli unici: {unique_symbols}")
            self.logger.info(f"   ⏰ Timeframe unici: {unique_timeframes}")

        except Exception as e:
            self.logger.error(f"Errore nelle statistiche database: {e}")

    def cleanup(self):
        """Pulizia risorse"""
        try:
            if self.connection:
                self.connection.close()
                self.logger.info("💾 Database chiuso")

            if self.mt5_connected:
                mt5.shutdown()
                self.logger.info("👋 Disconnesso da MT5")

        except Exception as e:
            self.logger.error(f"Errore nella pulizia: {e}")


def main():
    """Funzione principale"""
    print("🏗️  MT5 Historical Data Database Builder")
    print("=" * 50)

    # Crea il builder
    builder = MT5DatabaseBuilder()

    # Mostra configurazione attuale
    config = builder.load_config()
    print("\n📋 CONFIGURAZIONE ATTUALE:")
    print("-" * 30)
    print(f"📊 Database: {builder.db_path}")
    print(f"🔧 MT5 Path: {config.get('mt5_executable_path', 'Auto-detect')}")
    print(f"⏰ Timeframes: {config.get('timeframes', [])}")
    print(f"📈 Include Indices: {config.get('include_indices', True)}")
    print(f"💱 Include Forex: {config.get('include_forex', True)}")
    print(f"🏆 Include Commodities: {config.get('include_commodities', True)}")
    print(f"₿ Include Crypto: {config.get('include_crypto', True)}")
    print(f"🔍 Auto-detect simboli: {config.get('auto_detect_symbols', True)}")
    print(f"⚡ Max workers: {config.get('max_workers', 4)}")
    print(f"📊 Max bars per request: {config.get('max_bars_per_request', 50000):,}")

    # Conferma utente
    response = input(f"\n🚀 Vuoi procedere con la costruzione del database? (s/N): ").strip().lower()

    if response in ['s', 'si', 'sì', 'y', 'yes']:
        try:
            success = builder.build_database()
            if success:
                print("\n✅ Database costruito con successo!")
                print(f"📁 File database: {builder.db_path}")
                print(f"📄 Log file: mt5_database_builder.log")
            else:
                print("\n❌ Costruzione database fallita!")

        except KeyboardInterrupt:
            print("\n\n⚠️  Operazione interrotta dall'utente")
            builder.cleanup()
        except Exception as e:
            print(f"\n❌ Errore imprevisto: {e}")
            builder.cleanup()
    else:
        print("👋 Operazione annullata")


if __name__ == "__main__":
    main()