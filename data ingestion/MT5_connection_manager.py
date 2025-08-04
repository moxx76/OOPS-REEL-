import os
import winreg
import MetaTrader5 as mt5
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd


class MT5Manager:
    def __init__(self):
        self.mt5_installations = []
        self.connected_terminal = None

    def find_mt5_installations(self):
        """Trova tutte le installazioni di MT5 sul sistema"""
        installations = []

        # Metodo 1: Ricerca nel registro di Windows
        try:
            # Controlla HKEY_LOCAL_MACHINE
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall") as key:
                i = 0
                while True:
                    try:
                        subkey_name = winreg.EnumKey(key, i)
                        with winreg.OpenKey(key, subkey_name) as subkey:
                            try:
                                display_name = winreg.QueryValueEx(subkey, "DisplayName")[0]
                                if "MetaTrader 5" in display_name:
                                    install_location = winreg.QueryValueEx(subkey, "InstallLocation")[0]
                                    terminal_exe = os.path.join(install_location, "terminal64.exe")
                                    if os.path.exists(terminal_exe):
                                        installations.append({
                                            'name': display_name,
                                            'path': install_location,
                                            'executable': terminal_exe
                                        })
                            except FileNotFoundError:
                                pass
                        i += 1
                    except OSError:
                        break
        except Exception as e:
            print(f"Errore nella ricerca nel registro: {e}")

        # Metodo 2: Ricerca in percorsi comuni
        common_paths = [
            r"C:\Program Files\MetaTrader 5",
            r"C:\Program Files (x86)\MetaTrader 5",
            r"C:\Users\{}\AppData\Roaming\MetaQuotes\Terminal".format(os.getenv('USERNAME')),
        ]

        # Aggiungi anche possibili installazioni multiple
        for broker in ['ICMarkets', 'XM', 'Pepperstone', 'FTMO', 'Forex.com', 'OANDA']:
            common_paths.extend([
                f"C:\\Program Files\\{broker} MetaTrader 5",
                f"C:\\Program Files (x86)\\{broker} MetaTrader 5",
                f"C:\\Program Files\\MetaTrader 5 {broker}",
                f"C:\\Program Files (x86)\\MetaTrader 5 {broker}"
            ])

        for path in common_paths:
            if os.path.exists(path):
                terminal_exe = os.path.join(path, "terminal64.exe")
                if os.path.exists(terminal_exe):
                    # Controlla se non è già stato aggiunto
                    if not any(inst['path'] == path for inst in installations):
                        installations.append({
                            'name': f"MetaTrader 5 - {os.path.basename(path)}",
                            'path': path,
                            'executable': terminal_exe
                        })

        # Metodo 3: Ricerca ricorsiva nelle cartelle comuni
        search_roots = [
            "C:\\Program Files",
            "C:\\Program Files (x86)",
            f"C:\\Users\\{os.getenv('USERNAME')}\\AppData\\Roaming"
        ]

        for root in search_roots:
            if os.path.exists(root):
                for folder in os.listdir(root):
                    folder_path = os.path.join(root, folder)
                    if os.path.isdir(folder_path) and "metatrader" in folder.lower():
                        terminal_exe = os.path.join(folder_path, "terminal64.exe")
                        if os.path.exists(terminal_exe):
                            if not any(inst['path'] == folder_path for inst in installations):
                                installations.append({
                                    'name': f"MetaTrader 5 - {folder}",
                                    'path': folder_path,
                                    'executable': terminal_exe
                                })

        self.mt5_installations = installations
        return installations

    def select_installation(self):
        """Permette all'utente di selezionare quale installazione MT5 usare"""
        installations = self.find_mt5_installations()

        if not installations:
            print("❌ Nessuna installazione di MetaTrader 5 trovata!")
            return None

        print("\n🔍 Installazioni MetaTrader 5 trovate:")
        print("=" * 60)

        for i, installation in enumerate(installations, 1):
            print(f"{i}. {installation['name']}")
            print(f"   📁 Percorso: {installation['path']}")
            print(f"   🔧 Eseguibile: {installation['executable']}")
            print()

        while True:
            try:
                choice = input(f"Seleziona l'installazione da usare (1-{len(installations)}) o 'q' per uscire: ")

                if choice.lower() == 'q':
                    return None

                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(installations):
                    selected = installations[choice_idx]
                    print(f"\n✅ Selezionata: {selected['name']}")
                    return selected
                else:
                    print(f"❌ Inserisci un numero tra 1 e {len(installations)}")

            except ValueError:
                print("❌ Inserisci un numero valido o 'q' per uscire")

    def connect_to_mt5(self, installation_path=None):
        """Connette a MetaTrader 5"""
        if installation_path:
            # Connessione con percorso specifico
            if not mt5.initialize(path=installation_path):
                print(f"❌ Impossibile inizializzare MT5 con il percorso: {installation_path}")
                print(f"Errore: {mt5.last_error()}")
                return False
        else:
            # Connessione automatica
            if not mt5.initialize():
                print("❌ Impossibile inizializzare MT5")
                print(f"Errore: {mt5.last_error()}")
                return False

        # Verifica informazioni del terminale
        terminal_info = mt5.terminal_info()
        account_info = mt5.account_info()

        print("\n🎯 Connessione stabilita con successo!")
        print("=" * 50)
        print(f"📊 Terminale: {terminal_info.name}")
        print(f"🏢 Azienda: {terminal_info.company}")
        print(f"📍 Percorso: {terminal_info.path}")
        print(f"💼 Account: {account_info.login}")
        print(f"🏦 Server: {account_info.server}")
        print(f"💰 Bilancio: {account_info.balance} {account_info.currency}")

        self.connected_terminal = installation_path
        return True

    def check_data_availability(self, symbol="US30"):
        """Verifica la disponibilità dei dati per i principali timeframe"""
        if not mt5.terminal_info():
            print("❌ MT5 non è connesso!")
            return

        # Definisce i timeframe principali
        timeframes = {
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

        print(f"\n📈 Verifica disponibilità dati per {symbol}")
        print("=" * 60)

        results = []

        for tf_name, tf_value in timeframes.items():
            try:
                # Prova a ottenere le ultime 50000 barre (numero alto per testare il limite)
                rates = mt5.copy_rates_from_pos(symbol, tf_value, 0, 50000)

                if rates is not None and len(rates) > 0:
                    df = pd.DataFrame(rates)
                    df['time'] = pd.to_datetime(df['time'], unit='s')

                    # Informazioni sui dati
                    count = len(df)
                    first_date = df['time'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
                    last_date = df['time'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

                    # Calcola la copertura temporale
                    time_span = df['time'].iloc[-1] - df['time'].iloc[0]

                    results.append({
                        'timeframe': tf_name,
                        'bars_available': count,
                        'first_date': first_date,
                        'last_date': last_date,
                        'time_span_days': time_span.days,
                        'status': '✅'
                    })

                    print(f"{tf_name:>4} | {count:>8,} barre | {first_date} → {last_date} | {time_span.days:,} giorni")

                else:
                    results.append({
                        'timeframe': tf_name,
                        'bars_available': 0,
                        'first_date': 'N/A',
                        'last_date': 'N/A',
                        'time_span_days': 0,
                        'status': '❌'
                    })
                    print(f"{tf_name:>4} | {'0':>8,} barre | Nessun dato disponibile")

            except Exception as e:
                results.append({
                    'timeframe': tf_name,
                    'bars_available': 0,
                    'first_date': 'Errore',
                    'last_date': 'Errore',
                    'time_span_days': 0,
                    'status': '❌'
                })
                print(f"{tf_name:>4} | Errore: {str(e)}")

        # Riepilogo
        total_successful = sum(1 for r in results if r['status'] == '✅')
        print(f"\n📊 Riepilogo: {total_successful}/{len(timeframes)} timeframe disponibili")

        return results

    def get_available_symbols(self, limit=20, prefer_indices=True):
        """Ottiene la lista dei simboli disponibili, privilegiando gli indici"""
        if not mt5.terminal_info():
            print("❌ MT5 non è connesso!")
            return []

        symbols = mt5.symbols_get()
        if not symbols:
            print("❌ Nessun simbolo disponibile")
            return []

        # Filtra e organizza i simboli
        indices = []
        forex = []
        commodities = []
        crypto = []
        stocks = []
        others = []

        # Comuni simboli di indici
        index_patterns = [
            'US30', 'NAS100', 'SPX500', 'UK100', 'GER30', 'FRA40', 'JPN225',
            'AUS200', 'US500', 'USTEC', 'DJ30', 'DAX30', 'CAC40', 'FTSE',
            'NDX', 'SPY', 'QQQ', 'IWM', 'VIX'
        ]

        # Comuni simboli forex
        forex_patterns = [
            'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD',
            'NZDUSD', 'EURJPY', 'GBPJPY', 'EURGBP', 'CHFJPY', 'GBPCHF'
        ]

        # Commodities
        commodity_patterns = [
            'GOLD', 'SILVER', 'OIL', 'BRENT', 'XAUUSD', 'XAGUSD', 'USOIL', 'UKOIL'
        ]

        # Crypto
        crypto_patterns = [
            'BTC', 'ETH', 'LTC', 'XRP', 'ADA', 'DOT', 'BITCOIN', 'ETHEREUM'
        ]

        for symbol in symbols:
            name = symbol.name.upper()

            # Classifica i simboli
            if any(pattern in name for pattern in index_patterns):
                indices.append(symbol.name)
            elif any(pattern in name for pattern in forex_patterns):
                forex.append(symbol.name)
            elif any(pattern in name for pattern in commodity_patterns):
                commodities.append(symbol.name)
            elif any(pattern in name for pattern in crypto_patterns):
                crypto.append(symbol.name)
            elif len(name) <= 6 and name.isalpha():  # Probabilmente azioni
                stocks.append(symbol.name)
            else:
                others.append(symbol.name)

        if prefer_indices:
            print(f"\n📊 INDICI DISPONIBILI ({len(indices)} trovati):")
            print("=" * 50)
            for i, symbol in enumerate(indices[:15], 1):  # Mostra primi 15 indici
                print(f"{i:>2}. {symbol}")

            if forex:
                print(f"\n💱 FOREX PRINCIPALI ({len(forex)} trovati):")
                print("-" * 30)
                for i, symbol in enumerate(forex[:10], 1):  # Mostra primi 10 forex
                    print(f"{i:>2}. {symbol}")

            if commodities:
                print(f"\n🏆 COMMODITIES ({len(commodities)} trovati):")
                print("-" * 30)
                for i, symbol in enumerate(commodities[:8], 1):
                    print(f"{i:>2}. {symbol}")

            if crypto:
                print(f"\n₿ CRYPTO ({len(crypto)} trovati):")
                print("-" * 30)
                for i, symbol in enumerate(crypto[:8], 1):
                    print(f"{i:>2}. {symbol}")

        print(f"\n📈 RIEPILOGO TOTALE:")
        print(f"   📊 Indici: {len(indices)}")
        print(f"   💱 Forex: {len(forex)}")
        print(f"   🏆 Commodities: {len(commodities)}")
        print(f"   ₿ Crypto: {len(crypto)}")
        print(f"   📈 Azioni: {len(stocks)}")
        print(f"   📋 Altri: {len(others)}")
        print(f"   🔢 TOTALE: {len(symbols)}")

        # Restituisce prima gli indici, poi il resto
        return indices + forex + commodities + crypto + stocks + others

    def disconnect(self):
        """Disconnette da MetaTrader 5"""
        mt5.shutdown()
        print("\n👋 Disconnesso da MetaTrader 5")


def main():
    """Funzione principale"""
    print("🚀 MT5 Connection Manager")
    print("=" * 30)

    manager = MT5Manager()

    try:
        # Trova e seleziona installazione MT5
        selected_installation = manager.select_installation()

        if not selected_installation:
            print("👋 Operazione annullata dall'utente")
            return

        # Connetti a MT5
        success = manager.connect_to_mt5(selected_installation['executable'])

        if not success:
            return

        # Mostra simboli disponibili
        symbols = manager.get_available_symbols()

        # Chiedi all'utente quale simbolo analizzare
        if symbols:
            symbol_choice = input(f"\nInserisci il simbolo da analizzare (default: EURUSD): ").strip().upper()
            if not symbol_choice:
                symbol_choice = "EURUSD"

            # Verifica disponibilità dati
            manager.check_data_availability(symbol_choice)

        input("\nPremi Enter per continuare...")

    except KeyboardInterrupt:
        print("\n\n⚠️  Operazione interrotta dall'utente")
    except Exception as e:
        print(f"\n❌ Errore imprevisto: {e}")
    finally:
        manager.disconnect()


if __name__ == "__main__":
    main()