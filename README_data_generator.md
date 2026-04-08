# Automated Data Generator for AQS Grid Search Backtesting

A professional, user-friendly platform that automatically generates properly formatted CSV files for your Grid Search backtesting system. No manual data wrangling required!

## 🎯 What It Does

This platform eliminates the tedious work of:
- ❌ Manually fetching market data from multiple sources
- ❌ Finding and merging benchmark indices
- ❌ Calculating technical indicators
- ❌ Formatting CSVs to match GridSearch requirements
- ❌ Debugging data validation issues

Instead, you simply provide:
- ✅ Asset symbol (e.g., NVDA, AAPL, BITO)
- ✅ Time interval (e.g., 1h, 1d, 15min)
- ✅ Your name

**That's it!** The system does everything else automatically.

---

## 🚀 Quick Start (60 Seconds)

### Step 1: Install Dependencies
**Windows:**
```bash
pip install -r requirements.txt
```

**Mac/Linux:**
```bash
# Make sure virtual environment is activated first!
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 2: Start IB Gateway/TWS
- Launch Interactive Brokers Gateway or TWS
- Enable API connections in settings (Edit → Global Configuration → API)
- Note your port number (typically 7497 for TWS, 4002 for Gateway)

### Step 3: Run Generator

**Windows:**
```bash
python start_generator.py
```

**Mac/Linux:**
```bash
# Ensure virtual environment is activated first
source .venv/bin/activate
python start_generator.py
```

### Step 4: Follow Prompts
```
Enter asset symbol: NVDA
Enter interval: 1h
Enter your name: john
Use custom benchmarks? n
```

**Done!** Your file is ready at:
```
GridSearch_Data/merged_ibkr_NVDA_1h_john_05Dec2025.csv
```

---

## 🪟 Setup for Windows Users

### Initial Setup (One-Time)

**Step 1: Check Python Installation**
```bash
# Check if Python is installed
python --version

# If not installed, download from python.org
# Recommended: Python 3.8 or higher
```

**Step 2: Navigate to Project Directory**
```bash
# Change to your project folder
cd C:\Users\YourName\Documents\your-project-folder
```

**Step 3: Create Virtual Environment**
```bash
# Create virtual environment named '.venv'
python -m venv .venv
```

**Step 4: Activate Virtual Environment**
```bash
# Activate (you'll need to do this every time you open a new command prompt)
.venv\Scripts\activate

# You should see (.venv) appear at the start of your command prompt
```

**Step 5: Install Dependencies**
```bash
# Upgrade pip first
pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt

# Or install manually if needed:
# pip install ib_insync pandas numpy pytz matplotlib seaborn
```

**Step 6: Verify Installation**
```bash
# Test that all packages are installed correctly
python -c "import ib_insync; import pandas; import numpy; print('✓ All packages installed successfully!')"
```

### Daily Workflow (Windows)
```bash
# 1. Navigate to project folder
cd C:\Users\YourName\Documents\your-project-folder

# 2. Activate virtual environment (IMPORTANT - do this every time!)
.venv\Scripts\activate

# 3. Run the generator
python start_generator.py

# 4. When done, deactivate (optional)
deactivate
```

### Windows-Specific Notes
- Use backslashes `\` in file paths
- Default IB TWS port on Windows is usually 7497
- Virtual environment activation uses `.venv\Scripts\activate`
- Press Alt+F4 to quit applications
- Use Command Prompt or PowerShell

### VS Code Setup for Windows
1. Install VS Code from https://code.visualstudio.com/
2. Open VS Code and install Python extension (Ctrl+Shift+X, search "Python")
3. Open your project folder (File → Open Folder)
4. Select Python interpreter (Ctrl+Shift+P, type "Python: Select Interpreter")
5. Choose the interpreter from your `.venv` folder (`.\.venv\Scripts\python.exe`)

---

## 🍎 Setup for Mac Users

### Initial Setup (One-Time)

**Step 1: Check Python Installation**
```bash
# Check if Python 3 is installed
python3 --version

# If not installed, install via Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install python3
```

**Step 2: Navigate to Project Directory**
```bash
# Change to your project folder
cd ~/Documents/your-project-folder
# Or wherever you placed the project files
```

**Step 3: Create Virtual Environment**
```bash
# Create virtual environment named '.venv'
python3 -m venv .venv
```

**Step 4: Activate Virtual Environment**
```bash
# Activate (you'll need to do this every time you open a new terminal)
source .venv/bin/activate

# You should see (.venv) appear at the start of your command prompt
```

**Step 5: Install Dependencies**
```bash
# Upgrade pip first
pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt

# Or install manually if needed:
# pip install ib_insync pandas numpy pytz matplotlib seaborn
```

**Step 6: Verify Installation**
```bash
# Test that all packages are installed correctly
python3 -c "import ib_insync; import pandas; import numpy; print('✓ All packages installed successfully!')"
```

### Daily Workflow (Mac)
```bash
# 1. Navigate to project folder
cd ~/Documents/your-project-folder

# 2. Activate virtual environment (IMPORTANT - do this every time!)
source .venv/bin/activate

# 3. Run the generator
python start_generator.py

# 4. When done, deactivate (optional)
deactivate
```

### Mac-Specific Notes
- Use `python3` instead of `python` if both Python 2 and 3 are installed
- Virtual environment activation uses `source .venv/bin/activate` (not `.venv\Scripts\activate` like Windows)
- Use forward slashes `/` in paths, not backslashes `\`
- Default IB Gateway port on Mac is usually 4002
- Press ⌘+Q to quit applications (not Alt+F4)

### VS Code Setup for Mac
1. Install VS Code from https://code.visualstudio.com/
2. Open VS Code and install Python extension (⌘+Shift+X, search "Python")
3. Open your project folder (File → Open Folder)
4. Select Python interpreter (⌘+Shift+P, type "Python: Select Interpreter")
5. Choose the interpreter from your `.venv` folder (`./.venv/bin/python`)

---

## 📁 Project Structure
```
project/
│
├── start_generator.py          # Quick start launcher with checks
├── data_generator.py           # Main generation engine
├── README_data_generator.md     # Comprehensive user guide
├── requirements.txt            # Python dependencies
│
├── .venv/                       # Virtual environment
│   ├── Scripts/activate        # Activation script for Windows
│   └── bin/activate            # Activation script for Mac/Linux
│
├── GridSearch_Data/           # Output directory (auto-created)
│   └── merged_ibkr_*.csv      # Generated CSV files
│
├── AQS_SFGrid_parallel.py     # Your backtesting script (in parallel mode)
└── AQS_SFGridResults/         # Backtest results (from AQS_SFGrid_parallel.py)
```

---

## 🎨 Key Features

### 1. Intelligent Benchmark Selection
The system automatically chooses appropriate benchmarks:
```python
NVDA (tech stock)     → QQQ, XLK
BITO (crypto ETF)     → QQQ, GBTC
TECL (leveraged ETF)  → QQQ, SPY
DIS (entertainment)   → SPY, QQQ
```

### 2. Automatic Feature Engineering
Calculates essential features:
- Returns (price percentage change)
- Volatility (20-period rolling std)
- Volume ratios (volume / 20-period MA)
- Price spreads (asset vs benchmarks)

### 3. Built-in Validation
Checks output against GridSearch requirements:
- ✓ Required 'close' column exists
- ✓ At least one feature column present
- ✓ Numeric data types verified
- ✓ Minimum row count achieved
- ✓ NaN values handled

### 4. VIX Integration
Automatically includes CBOE Volatility Index for market context

### 5. Error Handling
Clear messages for common issues:
- Connection failures
- Missing data subscriptions
- Invalid symbols
- Insufficient historical data

---

## 📊 Supported Intervals

| Interval | Use Case | Data Fetched | Rows (approx) |
|----------|----------|--------------|---------------|
| 1min | Day trading | 1 day | 390 |
| 5min | Intraday | 5 days | 390 |
| 15min | Swing trading | 30 days | ~1,400 |
| **1h** | **Most common** | **2 years** | **~10,000** |
| 1d | Position trading | 10 years | ~2,500 |
| 1w | Long-term | 20 years | ~1,000 |

**Recommendation:** Use **1h interval** for most backtests (good balance of data volume and depth).

---

## 🎯 Usage Examples

### Example 1: Simple Tech Stock (Windows)
```bash
C:\> .venv\Scripts\activate
(.venv) C:\> python start_generator.py

Enter asset symbol: AAPL
Enter interval: 1h
Enter your name: alice
Use custom benchmarks? n

✓ Asset type detected: tech_stock
✓ Benchmarks selected: QQQ, XLK
✓ Fetched 10,234 bars for AAPL
✓ Fetched 10,234 bars for VIX
✓ Fetched 10,234 bars for QQQ
✓ Fetched 10,234 bars for XLK
✓ All validation checks passed
✓ Data saved to: GridSearch_Data/merged_ibkr_AAPL_1h_alice_05Dec2025.csv
```

### Example 2: Simple Tech Stock (Mac)
```bash
$ source .venv/bin/activate
(.venv) $ python start_generator.py

Enter asset symbol: AAPL
Enter interval: 1h
Enter your name: alice
Use custom benchmarks? n

✓ Asset type detected: tech_stock
✓ Benchmarks selected: QQQ, XLK
✓ All validation checks passed
✓ Data saved to: GridSearch_Data/merged_ibkr_AAPL_1h_alice_05Dec2025.csv
```

### Example 3: Crypto ETF with Defaults
```bash
$ python start_generator.py

Enter asset symbol: BITO
Enter interval: 15min
Enter your name: bob
Use custom benchmarks? n

✓ Asset type detected: crypto_etf
✓ Benchmarks selected: QQQ, GBTC
✓ Data saved to: GridSearch_Data/merged_ibkr_BITO_15min_bob_05Dec2025.csv
```

### Example 4: Custom Benchmarks
```bash
$ python start_generator.py

Enter asset symbol: DIS
Enter interval: 1d
Enter your name: carol
Use custom benchmarks? y
Enter benchmark symbols: SPY,XLY,NFLX

✓ Using custom benchmarks: SPY, XLY, NFLX
✓ Data saved to: GridSearch_Data/merged_ibkr_DIS_1d_carol_05Dec2025.csv
```

---

## 🔧 Configuration

### IB Connection Settings
Default settings work for most users:

**Windows (TWS - most common):**
- Host: 127.0.0.1
- Port: 7497
- Client ID: 1

**Mac (IB Gateway - more common):**
- Host: 127.0.0.1
- Port: 4002
- Client ID: 1

To change, edit `data_generator.py`:
```python
# For Windows users with TWS
generator.connect(host='127.0.0.1', port=7497, clientId=1)

# For Mac users with IB Gateway
generator.connect(host='127.0.0.1', port=4002, clientId=1)

# For Windows users with IB Gateway
generator.connect(host='127.0.0.1', port=4002, clientId=1)
```

### Adding Custom Benchmarks
Edit `BENCHMARK_MAP` in `data_generator.py`:
```python
BENCHMARK_MAP = {
    'tech_stock': ['QQQ', 'XLK', 'VGT'],  # Add more
    'my_custom_type': ['BENCH1', 'BENCH2'],
    # ...
}
```

### Custom Features
Add to `calculate_derived_features()`:
```python
# Add RSI
df['rsi_14'] = calculate_rsi(df['close'], 14)

# Add MACD
df['macd'] = calculate_macd(df['close'])
```

---

## 🔍 Output Format

### Generated CSV Structure
```csv
datetime,start_time,close,open,high,low,volume,VIX,QQQ_close,XLK_close,returns,volatility_20,close_QQQ_spread,volume_ma_ratio
2022-11-21 08:00:00-05:00,2022-11-21 08:00:00-05:00,26.92,26.52,26.97,26.51,1635.0,23.83,283.45,131.46,0.0044,-0.0100,-256.53,0.0117
...
```

### Column Descriptions

**Primary Data:**
- `datetime` - Timestamp with timezone
- `start_time` - Bar start time
- `open, high, low, close` - OHLC prices
- `volume` - Trading volume

**Indicators:**
- `VIX` - CBOE Volatility Index
- `[SYMBOL]_close` - Benchmark closing prices

**Features:**
- `returns` - Price returns
- `volatility_20` - 20-period volatility
- `volume_ma_ratio` - Volume / 20-period MA
- `close_[symbol]_spread` - Price spread to benchmark

---

## 🐛 Troubleshooting

### Connection Failed (Windows)
```
✗ Failed to connect to IB
```
**Windows Fixes:**
1. Start TWS (Trader Workstation)
2. Enable API: File → Global Configuration → API → Settings → Enable ActiveX and Socket Clients
3. Check port: Usually **7497** for TWS on Windows
4. Disable firewall temporarily: Windows Security → Firewall & network protection
5. Check if another app is using the port:
```bash
   netstat -ano | findstr :7497
```

### Connection Failed (Mac)
```
✗ Failed to connect to IB
```
**Mac Fixes:**
1. Start IB Gateway (not TWS) - more stable on Mac
2. Enable API: Configuration → API → Settings → Enable ActiveX and Socket Clients
3. Check port: Usually **4002** for IB Gateway on Mac (not 7497)
4. Disable firewall temporarily: System Preferences → Security & Privacy → Firewall
5. Check if another app is using the port:
```bash
   lsof -i :4002
```

### Python Command Not Found (Windows)
```bash
# Add Python to PATH during installation
# Or use full path:
C:\Python39\python.exe start_generator.py

# Or reinstall Python with "Add to PATH" checked
```

### Python Command Not Found (Mac)
```bash
# If 'python' doesn't work, use 'python3'
python3 start_generator.py

# Or create an alias (add to ~/.zshrc or ~/.bash_profile)
echo "alias python=python3" >> ~/.zshrc
source ~/.zshrc
```

### Virtual Environment Not Activating (Windows)
```bash
# Make sure you're using the correct command
.venv\Scripts\activate  # ✓ Correct

.venv/Scripts/activate  # ✗ Wrong (use backslash)
.\.venv\Scripts\activate # ✓ Also correct
```

### Virtual Environment Not Activating (Mac)
```bash
# Make sure you're using source, not just running the script
source .venv/bin/activate  # ✓ Correct

./.venv/bin/activate       # ✗ Wrong
.venv/bin/activate         # ✗ Wrong
```

### Permission Denied (Windows)
```bash
# Run Command Prompt as Administrator
# Right-click → Run as administrator

# Or change execution policy in PowerShell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Permission Denied (Mac)
```bash
# If you get permission errors
chmod +x start_generator.py

# Or use pip with --user flag
pip install --user -r requirements.txt
```

### No Data Received
```
✗ No data received for SYMBOL
```
**Fix (All Platforms):**
1. Verify symbol spelling (GOOGL vs GOOG)
2. Check market data subscription in IB account
3. Try different interval
4. Ensure market is open (or use RTH=False for extended hours)

### Insufficient Rows
```
⚠ Only 5,000 rows (minimum recommended: 9,060)
```
**Fix (All Platforms):**
1. Use longer interval (1h or 1d)
2. Accept warning for testing
3. Script will ask if you want to continue

---

## 🔗 Integration with GridSearch

### Step 1: Generate Data

**Windows:**
```bash
.venv\Scripts\activate
python start_generator.py
# Follow prompts...
```

**Mac:**
```bash
source .venv/bin/activate
python start_generator.py
# Follow prompts...
```

### Step 2: Update AQS_SFGrid.py

**Windows:**
```python
# Use raw string or double backslashes
file_path = r"GridSearch_Data\merged_ibkr_NVDA_1h_john_05Dec2025.csv"
# Or
file_path = "GridSearch_Data\\merged_ibkr_NVDA_1h_john_05Dec2025.csv"
```

**Mac:**
```python
# Use forward slashes
file_path = "GridSearch_Data/merged_ibkr_NVDA_1h_john_05Dec2025.csv"
```

**Cross-Platform (Recommended):**
```python
# Use os.path for cross-platform compatibility
import os
file_path = os.path.join("GridSearch_Data", "merged_ibkr_NVDA_1h_john_05Dec2025.csv")
```

### Step 3: Run Backtest

**Windows:**
```bash
.venv\Scripts\activate
python AQS_SFGrid.py
```

**Mac:**
```bash
source .venv/bin/activate
python AQS_SFGrid.py
```

The generated CSV is **100% compatible** with AQS_SFGrid.py requirements:
- ✓ Has required `close` column
- ✓ Has feature columns (returns, volatility, etc.)
- ✓ Proper format and data types
- ✓ Chronologically ordered
- ✓ No NaN values

---

## 📚 Documentation

- **Quick Start:** This README
- **Detailed Guide:** [README_data_generator.md](README_data_generator.md)

---

## 🎓 Best Practices

### ✓ DO:
- **Windows:** Always activate virtual environment (`.venv\Scripts\activate`)
- **Mac:** Always activate virtual environment (`source .venv/bin/activate`)
- Use 1h or 1d intervals for most backtests
- Let auto-detection choose benchmarks
- **Windows:** Keep TWS running during generation
- **Mac:** Keep IB Gateway running (more stable than TWS)
- Use meaningful creator names
- Review generated CSV before backtesting

### ✗ DON'T:
- **Windows:** Don't use forward slashes `/` in file paths
- **Mac:** Don't use backslashes `\` in file paths
- Use 1min for long-term analysis (too much data)
- Interrupt during data fetching
- Manually edit generated CSVs
- Run multiple generators simultaneously
- Use symbols without data subscriptions
- Forget to activate virtual environment!

---

## 🆘 Support

### Common Questions

**Q: How long does generation take?**
A: Usually 1-3 minutes depending on interval and data amount.

**Q: Can I generate data for multiple assets?**
A: Yes! Run the script multiple times, or use batch mode (coming soon).

**Q: What if I don't have IB account?**
A: You need an Interactive Brokers account and active data subscriptions.

**Q: Can I use this with other brokers?**
A: Currently IB only. Other brokers may be added in future versions.

**Q: How much historical data can I get?**
A: Depends on interval:
- 1min: 1 day
- 15min: ~30 days
- 1h: ~2 years
- 1d: ~10 years

**Q: Windows - Should I use TWS or IB Gateway?**
A: **TWS** is more common on Windows. Use port 7497.

**Q: Mac - Should I use TWS or IB Gateway?**
A: **IB Gateway** is recommended on Mac - it's more stable and lightweight. Use port 4002.

**Q: Do I need to activate the virtual environment every time?**
A: **Yes!** Every time you open a new terminal/command prompt.

---

## 🚦 Version & Status

**Version:** 1.0.0  
**Status:** Production Ready  
**Last Updated:** December 2025  

### Tested With:
- Python 3.8, 3.9, 3.10, 3.11
- ib_insync 0.9.86+
- pandas 1.5.0+
- Interactive Brokers TWS/Gateway
- **Windows:** 10, 11
- **macOS:** Ventura, Sonoma

---

## 📝 License

This tool is for educational and research purposes only. Always verify data before using in trading decisions. Past performance does not guarantee future results.

---

## 🎉 Getting Started Right Now

### Windows Users
```bash
# 1. Setup (one-time)
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

# 2. Daily workflow
cd C:\your-project-folder
.venv\Scripts\activate  # IMPORTANT!
python start_generator.py

# 3. Generate
# Enter NVDA, 1h, your name

# 4. Backtest
# Update AQS_SFGrid.py path and run

# That's it! 🚀
```

### Mac Users
```bash
# 1. Setup (one-time)
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 2. Daily workflow
cd ~/your-project-folder
source .venv/bin/activate  # IMPORTANT!
python start_generator.py

# 3. Generate
# Enter NVDA, 1h, your name

# 4. Backtest
# Update AQS_SFGrid.py path and run

# That's it! 🚀
```

---

## 🪟 Windows Quick Reference Card
```bash
# ═══════════════════════════════════════
#  WINDOWS QUICK REFERENCE
# ═══════════════════════════════════════

# Navigate to project
cd C:\Users\YourName\Documents\your-project-folder

# Activate virtual environment (DO THIS FIRST!)
.venv\Scripts\activate

# Run generator
python start_generator.py

# Run backtest
python AQS_SFGrid.py

# Deactivate when done
deactivate

# ═══════════════════════════════════════
#  COMMON WINDOWS ISSUES
# ═══════════════════════════════════════

# Check Python version
python --version

# Check which port TWS is using
netstat -ano | findstr :7497

# Run as Administrator (if permission issues)
# Right-click Command Prompt → Run as administrator

# Fix PowerShell execution policy
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# ═══════════════════════════════════════
```

---

## 🍎 Mac Quick Reference Card
```bash
# ═══════════════════════════════════════
#  MAC QUICK REFERENCE
# ═══════════════════════════════════════

# Navigate to project
cd ~/Documents/your-project-folder

# Activate virtual environment (DO THIS FIRST!)
source .venv/bin/activate

# Run generator
python start_generator.py

# Run backtest
python AQS_SFGrid.py

# Deactivate when done
deactivate

# ═══════════════════════════════════════
#  COMMON MAC ISSUES
# ═══════════════════════════════════════

# Check Python version
python3 --version

# Install Homebrew (if needed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python via Homebrew
brew install python3

# Check which port IB Gateway is using
lsof -i :4002

# Fix permissions
chmod +x start_generator.py

# ═══════════════════════════════════════
```