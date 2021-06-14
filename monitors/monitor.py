
# https://discuss.streamlit.io/t/can-i-animate-a-scatter-plot/2242/4

import time
import json
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from kafka import KafkaConsumer



def build_barplot_data(orderbook: dict, buy: bool = True):
    prices = sorted(orderbook.keys())
    depths = [sum(map(lambda order: order["order_size"], orderbook[price])) for price in prices]
    try:
        idx = depths.index(0)
        depths.pop(idx)
        prices.pop(idx)
    except:
        pass
    depths = [depth for _, depth in sorted(zip(prices, depths), key=lambda pair: int(pair[0]))]
    prices = list(map(lambda price: int(price), sorted(prices, key = lambda price: int(price))))
    colors = ["red" for i in range(len(prices))] if buy else ["blue" for i in range(len(prices))]
    bar_data = pd.DataFrame({'prices': prices, 'depths': depths, "colors": colors})
    return bar_data


if __name__ == "__main__":
    st_canvas = st.empty()
    kafka_config = {
        "bootstrap_servers": 'localhost:9092',
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
    }
    price_topic = "current-price"
    broadcast_consumer = KafkaConsumer(price_topic, **kafka_config)
    for msg in broadcast_consumer:
    feed = msg.value
    current_price = feed["fill_price"]
    fill_timestamp = feed["fill_timestamp"]
    if feed["level_2"] == {}:
        continue
    orderbooks = feed["level_2"]
    buy_book, sell_book = build_barplot_data(orderbooks["buy"]), build_barplot_data(orderbooks["sell"], buy=False)
    # concatenate two order books

    orderbook = pd.concat([buy_book, sell_book])
    # https://stackoverflow.com/questions/62579826/altair-bar-chart-assign-specific-colors-based-on-dataframe-column
    alt_chart = alt.Chart(orderbook, width = 1000, height = 600).mark_bar().encode(
        x='prices',
        y='depths',
        color="colors:N"
        )

    st_canvas.altair_chart(alt_chart)
