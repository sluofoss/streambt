# setup talib uv project
curl -LsSf https://astral.sh/uv/install.sh | sh
uv init
./install-talib.sh
uv add TA-Lib
uv add pyspark