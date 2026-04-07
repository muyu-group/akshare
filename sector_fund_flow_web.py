#!/usr/bin/env python3
"""板块资金流 Web 展示页，每 30 秒自动刷新一次。"""

from __future__ import annotations

import argparse
import json
import threading
import time
from datetime import date, datetime, time as dt_time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from sector_fund_flow_top10 import disable_system_proxy, fetch_sector_fund_flow

BOARD_CODE_CACHE: dict[str, dict[str, Any]] = {
    "行业资金流": {"ts": 0.0, "data": None},
    "概念资金流": {"ts": 0.0, "data": None},
}
TREND_BUCKET_MINUTES = 5
MAX_TREND_POINTS = 144
TREND_SERIES_PER_SIDE = 5
MARKET_OPEN_TIME = "09:30"
MARKET_CLOSE_TIME = "15:00"

HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>板块资金流监控</title>
  <style>
    :root {
      --bg: #0b1020;
      --panel: #121932;
      --panel-border: #243153;
      --text: #e8ecf3;
      --muted: #9aa6c1;
      --red: #ef5350;
      --blue: #42a5f5;
      --green: #26c281;
      --warn: #f7b955;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #0b1020 0%, #101830 100%);
      color: var(--text);
    }

    .container {
      width: min(1280px, 96vw);
      margin: 24px auto 40px;
    }

    .header, .panel {
      background: rgba(18, 25, 50, 0.92);
      border: 1px solid var(--panel-border);
      border-radius: 16px;
      box-shadow: 0 12px 32px rgba(0, 0, 0, 0.2);
    }

    .header {
      padding: 24px;
      margin-bottom: 20px;
    }

    h1 {
      margin: 0 0 8px;
      font-size: 30px;
    }

    .headline {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }

    .source-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 14px;
      border-radius: 999px;
      border: 1px solid rgba(38, 194, 129, 0.35);
      background: rgba(38, 194, 129, 0.12);
      color: #bff3db;
      font-size: 14px;
      font-weight: 600;
    }

    .source-badge.warn {
      border-color: rgba(247, 185, 85, 0.35);
      background: rgba(247, 185, 85, 0.12);
      color: #ffe3a8;
    }

    .subline {
      color: var(--muted);
      display: flex;
      flex-wrap: wrap;
      gap: 12px 24px;
      font-size: 14px;
    }

    .toolbar {
      margin-top: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }

    .toolbar-controls {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }

    .period-group {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .period-label {
      color: var(--muted);
      font-size: 14px;
    }

    .period-btn {
      border: 1px solid rgba(154, 166, 193, 0.25);
      background: rgba(255, 255, 255, 0.06);
      color: var(--text);
      border-radius: 999px;
      padding: 8px 14px;
      font-size: 14px;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    .period-btn:hover:not(.active):not(:disabled) {
      background: rgba(255, 255, 255, 0.1);
    }

    .period-btn.active {
      border-color: rgba(66, 165, 245, 0.5);
      background: rgba(66, 165, 245, 0.2);
      color: #d8ebff;
      box-shadow: 0 0 0 1px rgba(66, 165, 245, 0.15) inset;
    }

    .period-btn:disabled {
      opacity: 0.6;
      cursor: wait;
    }

    .refresh-select {
      border: 1px solid rgba(154, 166, 193, 0.25);
      background: rgba(255, 255, 255, 0.06);
      color: var(--text);
      border-radius: 10px;
      padding: 8px 34px 8px 12px;
      font-size: 14px;
      cursor: pointer;
      appearance: none;
      -webkit-appearance: none;
      -moz-appearance: none;
      background-image:
        linear-gradient(45deg, transparent 50%, #9aa6c1 50%),
        linear-gradient(135deg, #9aa6c1 50%, transparent 50%);
      background-position:
        calc(100% - 18px) calc(50% - 3px),
        calc(100% - 12px) calc(50% - 3px);
      background-size: 6px 6px, 6px 6px;
      background-repeat: no-repeat;
    }

    .refresh-select:disabled {
      opacity: 0.65;
      cursor: wait;
    }

    .indicator-note {
      color: #ffe3a8;
      font-size: 13px;
      display: none;
    }

    .refresh-btn {
      border: 1px solid rgba(66, 165, 245, 0.45);
      background: rgba(66, 165, 245, 0.16);
      color: var(--text);
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 14px;
      cursor: pointer;
      transition: background 0.2s ease, transform 0.2s ease, opacity 0.2s ease;
    }

    .refresh-btn:hover:not(:disabled) {
      background: rgba(66, 165, 245, 0.24);
      transform: translateY(-1px);
    }

    .refresh-btn:disabled {
      cursor: wait;
      opacity: 0.65;
    }

    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }

    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 12px rgba(38, 194, 129, 0.8);
    }

    .dot.warn {
      background: var(--warn);
      box-shadow: 0 0 12px rgba(247, 185, 85, 0.8);
    }

    .error {
      margin-top: 14px;
      padding: 12px 14px;
      background: rgba(239, 83, 80, 0.12);
      border: 1px solid rgba(239, 83, 80, 0.35);
      border-radius: 12px;
      color: #ffb4b2;
      display: none;
      white-space: pre-wrap;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 20px;
      align-items: start;
    }

    .panel {
      padding: 20px;
    }

    .panel h2 {
      margin: 0 0 18px;
      font-size: 22px;
    }

    .list {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .panel-stack {
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    .subpanel h2 {
      margin-bottom: 16px;
    }

    .item {
      border: 1px solid rgba(154, 166, 193, 0.16);
      border-radius: 14px;
      padding: 14px;
      background: rgba(255, 255, 255, 0.02);
    }

    .item-top {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 10px;
    }

    .rank-name {
      font-size: 16px;
      font-weight: 600;
    }

    .value {
      font-size: 18px;
      font-weight: 700;
      white-space: nowrap;
    }

    .bar-track {
      width: 100%;
      height: 10px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.06);
      overflow: hidden;
      margin-bottom: 10px;
    }

    .bar {
      height: 100%;
      border-radius: 999px;
    }

    .meta {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 16px;
      color: var(--muted);
      font-size: 13px;
    }

    .empty {
      color: var(--muted);
      padding: 32px 0;
      text-align: center;
    }

    .chart-panel {
      min-height: 100%;
    }

    .chart-panel-stack {
      display: flex;
      flex-direction: column;
      gap: 20px;
    }

    .chart-headline {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }

    .chart-summary {
      color: var(--muted);
      font-size: 13px;
    }

    .chart-note {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 14px;
    }

    .chart-wrap {
      position: relative;
      width: 100%;
      height: 420px;
      border: 1px solid rgba(154, 166, 193, 0.16);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.02);
      overflow: hidden;
    }

    .chart-canvas {
      display: block;
      width: 100%;
      height: 100%;
    }

    .chart-empty {
      position: absolute;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--muted);
      padding: 24px;
      text-align: center;
      background: rgba(18, 25, 50, 0.55);
    }

    .speed-panel h2 {
      margin-bottom: 8px;
    }

    .speed-note {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 14px;
    }

    .speed-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .speed-value {
      color: #ef5350;
      font-size: 18px;
      font-weight: 700;
      white-space: nowrap;
    }

    .speed-meta {
      color: var(--muted);
      font-size: 13px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }

    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
      .meta { grid-template-columns: 1fr; }
      h1 { font-size: 24px; }
      .chart-wrap { height: 340px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <section class="header">
      <div class="headline">
        <h1>板块资金流监控</h1>
        <div id="sourceBadge" class="source-badge">当前数据源：加载中</div>
      </div>
      <div class="subline">
        <span id="scope">加载中...</span>
        <span id="refreshLabel">刷新频率：30秒</span>
        <span id="updatedAt">最近更新：-</span>
        <span class="status"><span id="statusDot" class="dot"></span><span id="statusText">初始化中</span></span>
      </div>
      <div class="toolbar">
        <div class="toolbar-controls">
          <div class="period-group">
            <span class="period-label">查询周期</span>
            <button class="period-btn" type="button" data-period="today">当日</button>
            <button class="period-btn" type="button" data-period="3">3日</button>
            <button class="period-btn" type="button" data-period="5">5日</button>
            <button class="period-btn" type="button" data-period="7">7日</button>
          </div>
          <div class="period-group">
            <span class="period-label">刷新频率</span>
            <select id="refreshIntervalSelect" class="refresh-select">
              <option value="30000">30秒</option>
              <option value="60000">1分钟</option>
              <option value="120000">2分钟</option>
              <option value="300000">5分钟</option>
            </select>
          </div>
          <span id="indicatorNote" class="indicator-note"></span>
        </div>
        <button id="refreshBtn" class="refresh-btn" type="button">立即刷新</button>
      </div>
      <div id="errorBox" class="error"></div>
    </section>

    <section class="grid">
      <div class="panel">
        <div class="panel-stack">
          <section class="subpanel">
            <h2 id="inflowTitle">主力资金净流入</h2>
            <div id="inflowList" class="list"><div class="empty">加载中...</div></div>
          </section>
          <section class="subpanel">
            <h2 id="outflowTitle">主力资金净流出</h2>
            <div id="outflowList" class="list"><div class="empty">加载中...</div></div>
          </section>
        </div>
      </div>
      <div class="panel chart-panel">
        <div class="chart-panel-stack">
          <section class="subpanel">
            <div class="chart-headline">
              <h2>板块资金流向</h2>
              <div id="trendSummary" class="chart-summary">加载中...</div>
            </div>
            <div class="chart-note">按 5 分钟间隔记录板块净额快照，左侧为金额，右侧为板块名称；每个板块一条净额折线。</div>
            <div class="chart-wrap">
              <canvas id="trendCanvas" class="chart-canvas"></canvas>
              <div id="trendEmpty" class="chart-empty">加载中...</div>
            </div>
          </section>
          <section class="subpanel speed-panel">
            <div class="chart-headline">
              <h2>资金流入速度 Top5</h2>
              <div id="speedSummary" class="chart-summary">加载中...</div>
            </div>
            <div class="speed-note">按最近两个有效 5 分钟采样点计算净额增速，展示资金流入速度最快的板块。</div>
            <div id="speedList" class="speed-list"><div class="empty">加载中...</div></div>
          </section>
        </div>
      </div>
    </section>
  </div>

  <script>
    const DEFAULT_INDICATOR = "today";
    const PERIOD_STORAGE_KEY = "sector-fund-flow-period";
    const REFRESH_INTERVAL_STORAGE_KEY = "sector-fund-flow-refresh-ms";
    const REFRESH_OPTIONS = {
      30000: "30秒",
      60000: "1分钟",
      120000: "2分钟",
      300000: "5分钟"
    };
    const DEFAULT_REFRESH_MS = 30000;
    let currentIndicator = DEFAULT_INDICATOR;
    let currentRefreshMs = DEFAULT_REFRESH_MS;
    let refreshing = false;
    let activeController = null;
    let latestTrend = null;
    let refreshTimer = null;

    function formatYi(value) {
      const abs = Math.abs(value);
      return `${value >= 0 ? "" : "-"}${abs.toFixed(2)} 亿`;
    }

    function formatOptionalYi(value) {
      if (value === null || value === undefined) {
        return "-";
      }
      return formatYi(value);
    }

    function escapeHtml(text) {
      return String(text)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function providerLabel(provider) {
      const labels = {
        auto: "自动切换",
        eastmoney: "东方财富",
        ths: "同花顺"
      };
      return labels[provider] || provider;
    }

    function indicatorCodeToLabel(code) {
      const labels = {
        "today": "当日",
        "3": "3日",
        "5": "5日",
        "7": "7日"
      };
      return labels[code] || code;
    }

    function isValidPeriod(code) {
      return ["today", "3", "5", "7"].includes(code);
    }

    function isValidRefreshMs(value) {
      return Object.prototype.hasOwnProperty.call(REFRESH_OPTIONS, String(value));
    }

    function syncPeriodState(period) {
      if (!isValidPeriod(period)) {
        return;
      }
      currentIndicator = period;
      const url = new URL(window.location.href);
      url.searchParams.set("period", period);
      window.history.replaceState({}, "", url);
      window.localStorage.setItem(PERIOD_STORAGE_KEY, period);
    }

    function getInitialPeriod() {
      const url = new URL(window.location.href);
      const fromUrl = url.searchParams.get("period");
      if (isValidPeriod(fromUrl)) {
        return fromUrl;
      }
      const fromStorage = window.localStorage.getItem(PERIOD_STORAGE_KEY);
      if (isValidPeriod(fromStorage)) {
        return fromStorage;
      }
      return DEFAULT_INDICATOR;
    }

    function getInitialRefreshMs() {
      const url = new URL(window.location.href);
      const fromUrl = url.searchParams.get("refreshMs");
      if (isValidRefreshMs(fromUrl)) {
        return Number(fromUrl);
      }
      const fromStorage = window.localStorage.getItem(REFRESH_INTERVAL_STORAGE_KEY);
      if (isValidRefreshMs(fromStorage)) {
        return Number(fromStorage);
      }
      return DEFAULT_REFRESH_MS;
    }

    function syncRefreshState(refreshMs) {
      if (!isValidRefreshMs(refreshMs)) {
        return;
      }
      currentRefreshMs = Number(refreshMs);
      const label = REFRESH_OPTIONS[String(currentRefreshMs)];
      const url = new URL(window.location.href);
      url.searchParams.set("refreshMs", String(currentRefreshMs));
      window.history.replaceState({}, "", url);
      window.localStorage.setItem(REFRESH_INTERVAL_STORAGE_KEY, String(currentRefreshMs));
      document.getElementById("refreshLabel").textContent = `刷新频率：${label}`;
      document.getElementById("refreshIntervalSelect").value = String(currentRefreshMs);
    }

    function resetRefreshTimer() {
      if (refreshTimer) {
        window.clearInterval(refreshTimer);
      }
      refreshTimer = window.setInterval(refresh, currentRefreshMs);
    }

    function setActivePeriodButton(period) {
      document.querySelectorAll(".period-btn").forEach((button) => {
        button.classList.toggle("active", button.dataset.period === period);
      });
    }

    function renderList(containerId, items, color) {
      const container = document.getElementById(containerId);
      if (!items.length) {
        container.innerHTML = '<div class="empty">暂无数据</div>';
        return;
      }

      const max = Math.max(...items.map(item => Math.abs(item.net_inflow_yi)), 1);
      container.innerHTML = items.map((item, index) => {
        const width = Math.max((Math.abs(item.net_inflow_yi) / max) * 100, 2);
        return `
          <div class="item">
            <div class="item-top">
              <div class="rank-name">${index + 1}. ${escapeHtml(item.name)}</div>
              <div class="value" style="color:${color}">${formatYi(item.net_inflow_yi)}</div>
            </div>
            <div class="bar-track">
              <div class="bar" style="width:${width}%; background:${color}"></div>
            </div>
            <div class="meta">
              <div>涨跌幅：${item.change_pct}%</div>
              <div>净占比：${item.net_ratio}</div>
              <div>近3日净额：${formatOptionalYi(item.recent_3day_net_yi)}</div>
              <div>龙一：${escapeHtml(item.leader_1 || "-")}</div>
              <div>龙二：${escapeHtml(item.leader_2 || "-")}</div>
              <div>关注股1：${escapeHtml(item.watch_stock_1 || "-")}</div>
              <div>关注股2：${escapeHtml(item.watch_stock_2 || "-")}</div>
            </div>
          </div>
        `;
      }).join("");
    }

    function renderTrendChart(trend) {
      latestTrend = trend;
      const canvas = document.getElementById("trendCanvas");
      const emptyBox = document.getElementById("trendEmpty");
      const summary = document.getElementById("trendSummary");
      const times = trend?.times || [];
      const seriesList = trend?.series || [];

      if (!times.length || !seriesList.length) {
        const context = canvas.getContext("2d");
        context.clearRect(0, 0, canvas.width, canvas.height);
        emptyBox.style.display = "flex";
        emptyBox.textContent = "暂无可用的板块资金流向数据";
        summary.textContent = "等待首个 5 分钟周期采样";
        return;
      }

      emptyBox.style.display = "none";
      summary.textContent = `最近一档 ${times[times.length - 1]}，展示 ${seriesList.length} 个板块净额轨迹`;

      const rect = canvas.getBoundingClientRect();
      const width = Math.max(rect.width, 320);
      const height = Math.max(rect.height, 280);
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);

      const context = canvas.getContext("2d");
      context.setTransform(dpr, 0, 0, dpr, 0, 0);
      context.clearRect(0, 0, width, height);

      const padding = { top: 20, right: 118, bottom: 36, left: 64 };
      const plotWidth = width - padding.left - padding.right;
      const plotHeight = height - padding.top - padding.bottom;
      const values = seriesList.flatMap((item) => item.values.filter((value) => typeof value === "number"));
      let maxAbs = Math.max(...values.map((value) => Math.abs(value)), 1);
      maxAbs = Math.ceil(maxAbs * 1.15 * 10) / 10;
      const minY = -maxAbs;
      const maxY = maxAbs;
      const yTicks = [maxAbs, maxAbs / 2, 0, -maxAbs / 2, -maxAbs];
      const palette = [
        "#ef5350",
        "#42a5f5",
        "#26c281",
        "#f7b955",
        "#ab47bc",
        "#ff7043",
        "#7e57c2",
        "#26a69a",
        "#ec407a",
        "#8d6e63",
      ];

      function xAt(index) {
        if (times.length === 1) {
          return padding.left + plotWidth / 2;
        }
        return padding.left + (plotWidth * index) / (times.length - 1);
      }

      function yAt(value) {
        const ratio = (value - minY) / (maxY - minY || 1);
        return padding.top + plotHeight - ratio * plotHeight;
      }

      context.strokeStyle = "rgba(154, 166, 193, 0.18)";
      context.fillStyle = "#9aa6c1";
      context.font = '12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
      context.textAlign = "right";
      context.textBaseline = "middle";

      yTicks.forEach((tick) => {
        const y = yAt(tick);
        context.lineWidth = tick === 0 ? 1.5 : 1;
        context.beginPath();
        context.moveTo(padding.left, y);
        context.lineTo(width - padding.right, y);
        context.stroke();
        context.fillText(`${tick.toFixed(tick === 0 ? 0 : 1)} 亿`, padding.left - 8, y);
      });

      context.textAlign = "center";
      context.textBaseline = "top";
      const labelStep = Math.max(1, Math.ceil(times.length / 6));
      times.forEach((time, index) => {
        if (index % labelStep !== 0 && index !== times.length - 1) {
          return;
        }
        const x = xAt(index);
        context.fillText(time, x, height - padding.bottom + 10);
      });

      function drawSeries(series, color) {
        context.strokeStyle = color;
        context.fillStyle = color;
        context.lineWidth = 2;
        let started = false;
        let lastValid = null;
        context.beginPath();
        series.values.forEach((value, index) => {
          if (typeof value !== "number") {
            return;
          }
          const x = xAt(index);
          const y = yAt(value);
          if (!started) {
            context.moveTo(x, y);
            started = true;
          } else {
            context.lineTo(x, y);
          }
          lastValid = { x, y, index, value };
        });
        if (started) {
          context.stroke();
        }

        series.values.forEach((value, index) => {
          if (typeof value !== "number") {
            return;
          }
          const x = xAt(index);
          const y = yAt(value);
          context.beginPath();
          context.arc(x, y, lastValid && index === lastValid.index ? 4 : 2.5, 0, Math.PI * 2);
          context.fill();
        });

        return lastValid;
      }

      const labels = [];
      seriesList.forEach((series, index) => {
        const color = palette[index % palette.length];
        const lastValid = drawSeries(series, color);
        if (lastValid) {
          labels.push({
            name: series.name,
            color,
            value: lastValid.value,
            x: lastValid.x,
            y: lastValid.y,
          });
        }
      });

      labels.sort((a, b) => a.y - b.y);
      const minGap = 16;
      for (let index = 1; index < labels.length; index += 1) {
        labels[index].y = Math.max(labels[index].y, labels[index - 1].y + minGap);
      }
      for (let index = labels.length - 2; index >= 0; index -= 1) {
        labels[index].y = Math.min(labels[index].y, labels[index + 1].y - minGap);
      }

      const minLabelY = padding.top + 10;
      const maxLabelY = height - padding.bottom - 10;
      labels.forEach((label) => {
        label.y = Math.min(Math.max(label.y, minLabelY), maxLabelY);
      });

      context.textAlign = "left";
      context.textBaseline = "middle";
      context.font = '12px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
      labels.forEach((label) => {
        const labelX = width - padding.right + 14;
        context.strokeStyle = label.color;
        context.fillStyle = label.color;
        context.lineWidth = 1;
        context.beginPath();
        context.moveTo(label.x + 4, label.y);
        context.lineTo(labelX - 6, label.y);
        context.stroke();
        context.fillText(`${label.name} ${label.value > 0 ? "+" : ""}${label.value.toFixed(2)}`, labelX, label.y);
      });
    }

    function renderSpeedRanking(items) {
      const container = document.getElementById("speedList");
      const summary = document.getElementById("speedSummary");
      if (!items || !items.length) {
        container.innerHTML = '<div class="empty">暂无足够的历史采样，无法计算流入速度</div>';
        summary.textContent = "等待至少两个有效采样点";
        return;
      }

      summary.textContent = `最近区间：${items[0].from_time} → ${items[0].to_time}`;
      const maxSpeed = Math.max(...items.map((item) => item.speed_yi_per_min), 1);
      container.innerHTML = items.map((item, index) => {
        const width = Math.max((item.speed_yi_per_min / maxSpeed) * 100, 2);
        return `
          <div class="item">
            <div class="item-top">
              <div class="rank-name">${index + 1}. ${escapeHtml(item.name)}</div>
              <div class="speed-value">+${item.speed_yi_per_min.toFixed(2)} 亿/分钟</div>
            </div>
            <div class="bar-track">
              <div class="bar" style="width:${width}%; background:#ef5350"></div>
            </div>
            <div class="speed-meta">
              <div>区间净流入增量：+${item.delta_yi.toFixed(2)} 亿</div>
              <div>采样区间：${item.from_time} → ${item.to_time}</div>
            </div>
          </div>
        `;
      }).join("");
    }

    async function loadData(signal) {
      const query = new URLSearchParams({ period: currentIndicator });
      const response = await fetch(`/api/data?${query.toString()}`, {
        cache: "no-store",
        signal,
      });
      if (!response.ok) {
        throw new Error(`请求失败: ${response.status}`);
      }
      return response.json();
    }

    function setRefreshingState(isRefreshing) {
      const refreshBtn = document.getElementById("refreshBtn");
      const refreshSelect = document.getElementById("refreshIntervalSelect");
      refreshBtn.disabled = isRefreshing;
      refreshSelect.disabled = isRefreshing;
      refreshBtn.textContent = isRefreshing ? "刷新中..." : "立即刷新";
    }

    async function refresh(force = false) {
      if (refreshing) {
        if (!force) {
          return;
        }
        if (activeController) {
          activeController.abort();
        }
      }

      refreshing = true;
      setRefreshingState(true);
      const controller = new AbortController();
      activeController = controller;

      const errorBox = document.getElementById("errorBox");
      const statusDot = document.getElementById("statusDot");
      const statusText = document.getElementById("statusText");
      const sourceBadge = document.getElementById("sourceBadge");
      const indicatorNote = document.getElementById("indicatorNote");

      try {
        const data = await loadData(controller.signal);
        syncPeriodState(data.requested_indicator_code);
        setActivePeriodButton(currentIndicator);
        document.getElementById("scope").textContent =
          `范围：${data.requested_indicator} / ${data.sector_type} / Top ${data.top}`;
        document.getElementById("updatedAt").textContent =
          `最近更新：${data.fetched_at}`;
        document.getElementById("inflowTitle").textContent =
          `主力资金净流入 Top${data.top}`;
        document.getElementById("outflowTitle").textContent =
          `主力资金净流出 Top${data.top}`;

        renderList("inflowList", data.inflow, "#ef5350");
        renderList("outflowList", data.outflow, "#42a5f5");
        renderTrendChart(data.trend);
        renderSpeedRanking(data.speed_ranking);
        sourceBadge.textContent = `当前数据源：${providerLabel(data.provider)}`;
        if (data.indicator_note) {
          indicatorNote.style.display = "inline";
          indicatorNote.textContent = data.indicator_note;
        } else {
          indicatorNote.style.display = "none";
          indicatorNote.textContent = "";
        }

        if (data.source === "live") {
          statusDot.className = "dot";
          statusText.textContent = "实时数据";
          sourceBadge.className = "source-badge";
        } else {
          statusDot.className = "dot warn";
          statusText.textContent = "快照回退";
          sourceBadge.className = "source-badge warn";
        }

        if (data.error) {
          errorBox.style.display = "block";
          errorBox.textContent = data.error;
        } else {
          errorBox.style.display = "none";
          errorBox.textContent = "";
        }
      } catch (error) {
        if (error.name === "AbortError") {
          return;
        }
        statusDot.className = "dot warn";
        statusText.textContent = "获取失败";
        sourceBadge.className = "source-badge warn";
        sourceBadge.textContent = "当前数据源：获取失败";
        errorBox.style.display = "block";
        errorBox.textContent = `页面刷新失败：${error.message}`;
        renderTrendChart(latestTrend);
        renderSpeedRanking([]);
      } finally {
        if (activeController === controller) {
          activeController = null;
          refreshing = false;
          setRefreshingState(false);
          setActivePeriodButton(currentIndicator);
        }
      }
    }

    document.querySelectorAll(".period-btn").forEach((button) => {
      button.addEventListener("click", () => {
        syncPeriodState(button.dataset.period);
        setActivePeriodButton(currentIndicator);
        refresh(true);
      });
    });
    document.getElementById("refreshIntervalSelect").addEventListener("change", (event) => {
      syncRefreshState(event.target.value);
      resetRefreshTimer();
    });
    document.getElementById("refreshBtn").addEventListener("click", refresh);
    window.addEventListener("resize", () => {
      if (latestTrend) {
        renderTrendChart(latestTrend);
      }
    });
    syncPeriodState(getInitialPeriod());
    syncRefreshState(getInitialRefreshMs());
    setActivePeriodButton(currentIndicator);
    resetRefreshTimer();
    refresh();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="板块资金流 Web 页面")
    parser.add_argument("--indicator", default="今日", choices=["今日", "3日", "5日", "10日"])
    parser.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "eastmoney", "ths"],
        help="数据源，默认: auto（先同花顺，失败后回退东方财富）",
    )
    parser.add_argument(
        "--sector-type",
        default="行业资金流",
        choices=["行业资金流", "概念资金流", "地域资金流"],
    )
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--cache-file",
        default="sector_fund_flow_cache.json",
        help="最近一次成功抓取结果缓存文件",
    )
    return parser.parse_args()


def format_ratio(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    if not text or text == "-":
        return "-"
    return text if text.endswith("%") else f"{text}%"


def build_recent_3day_map(sector_type: str, provider: str) -> dict[str, float]:
    fallback_provider = provider
    if provider == "eastmoney":
        fallback_provider = "ths"

    try:
        df, _ = fetch_sector_fund_flow("3日", sector_type, provider=fallback_provider)
    except Exception:
        return {}

    result: dict[str, float] = {}
    for row in df.itertuples(index=False):
        result[str(row.板块名称)] = float(row.主力净流入)
    return result


def parse_amount_to_yi(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text == "-":
        return None
    try:
        if text.endswith("亿"):
            return float(text[:-1])
        if text.endswith("万"):
            return float(text[:-1]) / 10000
        return float(text)
    except ValueError:
        return None


def summarize_fetch_error(error: Exception) -> str:
    text = str(error)
    if "403" in text or "Nginx forbidden" in text:
        return "同花顺当前拒绝了这台机器的访问请求，页面已自动回退到最近一次成功快照。"
    if "RemoteDisconnected" in text or "Connection aborted" in text:
        return "东方财富连接被远端关闭，页面已自动回退到最近一次成功快照。"
    if "No tables found" in text or "'NoneType' object has no attribute 'text'" in text:
        return "同花顺返回了拦截页或空页面，页面已自动回退到最近一次成功快照。"
    return f"实时拉取失败，页面已自动回退到最近一次成功快照：{text}"


def load_board_code_df(sector_type: str, ttl_seconds: int = 600) -> Any:
    import akshare as ak

    if sector_type == "行业资金流":
        loader = ak.stock_board_industry_name_ths
    elif sector_type == "概念资金流":
        loader = ak.stock_board_concept_name_ths
    else:
        return None

    cached = BOARD_CODE_CACHE.get(sector_type)
    now = time.time()
    if cached and cached["data"] is not None and now - cached["ts"] < ttl_seconds:
        return cached["data"]

    last_error = None
    for _ in range(3):
        try:
            code_df = loader()
            BOARD_CODE_CACHE[sector_type] = {"ts": time.time(), "data": code_df}
            return code_df
        except Exception as exc:
            last_error = exc
            time.sleep(0.6)

    if cached and cached["data"] is not None:
        return cached["data"]
    raise RuntimeError(last_error) from last_error


def fetch_board_detail_ths(sector_type: str, board_name: str) -> dict[str, str]:
    import pandas as pd
    import requests

    if sector_type == "行业资金流":
        url_prefix = "https://q.10jqka.com.cn/thshy/detail/code"
    elif sector_type == "概念资金流":
        url_prefix = "https://q.10jqka.com.cn/gn/detail/code"
    else:
        return {
            "leader_1": "-",
            "leader_2": "-",
            "watch_stock_1": "-",
            "watch_stock_2": "-",
        }

    code_df = load_board_code_df(sector_type)

    matched = code_df[code_df["name"] == board_name]
    if matched.empty:
        return {
            "leader_1": "-",
            "leader_2": "-",
            "watch_stock_1": "-",
            "watch_stock_2": "-",
        }

    board_code = str(matched.iloc[0]["code"])
    url = f"{url_prefix}/{board_code}/"
    response_text = None
    last_error = None
    for _ in range(3):
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"
            if response.text:
                response_text = response.text
                break
            last_error = ValueError("板块详情页返回空内容")
        except Exception as exc:
            last_error = exc
            time.sleep(0.6)

    if not response_text:
        raise RuntimeError(last_error) from last_error

    tables = pd.read_html(StringIO(response_text))
    if not tables:
        return {
            "leader_1": "-",
            "leader_2": "-",
            "watch_stock_1": "-",
            "watch_stock_2": "-",
        }

    detail_df = tables[0].copy()
    change_col = "涨跌幅(%)"
    amount_col = "成交额"
    code_col = "代码"
    name_col = "名称"

    detail_df[change_col] = pd.to_numeric(detail_df[change_col], errors="coerce")
    detail_df["成交额(亿)"] = detail_df[amount_col].map(parse_amount_to_yi)
    detail_df[code_col] = detail_df[code_col].astype(str).str.zfill(6)

    def format_stock(row: Any) -> str:
        return f"{row[name_col]}({row[code_col]})"

    leaders_df = detail_df.sort_values(
        by=[change_col, "成交额(亿)"],
        ascending=[False, False],
        na_position="last",
    ).head(2)
    leaders = [format_stock(row) for _, row in leaders_df.iterrows()]

    watch_df = detail_df.copy()
    if not leaders_df.empty:
        watch_df = watch_df[~watch_df[code_col].isin(leaders_df[code_col])]
    positive_watch_df = watch_df[watch_df[change_col].fillna(-999) > 0]
    if positive_watch_df.empty:
        positive_watch_df = watch_df
    watch_df = positive_watch_df.sort_values(
        by=["成交额(亿)", change_col],
        ascending=[False, False],
        na_position="last",
    ).head(2)
    watch_list = [format_stock(row) for _, row in watch_df.iterrows()]

    while len(leaders) < 2:
        leaders.append("-")
    while len(watch_list) < 2:
        watch_list.append("-")

    return {
        "leader_1": leaders[0],
        "leader_2": leaders[1],
        "watch_stock_1": watch_list[0],
        "watch_stock_2": watch_list[1],
    }


def build_board_detail_map(
    sector_type: str,
    board_names: list[str],
    cache: dict[str, dict[str, Any]],
    ttl_seconds: int = 120,
) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    now = time.time()
    for board_name in board_names:
        cached = cache.get(board_name)
        if cached and now - cached.get("ts", 0) < ttl_seconds:
            result[board_name] = cached["data"]
            continue
        try:
            data = fetch_board_detail_ths(sector_type, board_name)
        except Exception:
            if cached and cached.get("data"):
                data = cached["data"]
            else:
                data = {
                    "leader_1": "-",
                    "leader_2": "-",
                    "watch_stock_1": "-",
                    "watch_stock_2": "-",
                }
        cache[board_name] = {"ts": now, "data": data}
        result[board_name] = data
    return result


def period_code_to_indicator(period_code: str | None) -> str | None:
    mapping = {
        "today": "今日",
        "3": "3日",
        "5": "5日",
        "7": "7日",
    }
    if period_code is None:
        return None
    return mapping.get(period_code)


def period_code_to_indicator_code(indicator: str | None) -> str | None:
    mapping = {
        "今日": "today",
        "3日": "3",
        "5日": "5",
        "7日": "7",
        "10日": "7",
    }
    if indicator is None:
        return None
    return mapping.get(indicator)


def resolve_requested_indicator(requested_indicator: str) -> tuple[str, str | None]:
    if requested_indicator in {"3日", "5日", "10日", "今日"}:
        return requested_indicator, None
    if requested_indicator == "7日":
        return "10日", "7日暂无直连接口，当前按10日数据近似展示"
    raise ValueError(f"不支持的查询周期: {requested_indicator}")


def select_trend_boards(df: Any) -> list[str]:
    inflow_names = (
        df.sort_values("主力净流入", ascending=False)
        .head(TREND_SERIES_PER_SIDE)["板块名称"]
        .astype(str)
        .tolist()
    )
    outflow_names = (
        df.sort_values("主力净流入", ascending=True)
        .head(TREND_SERIES_PER_SIDE)["板块名称"]
        .astype(str)
        .tolist()
    )
    return list(dict.fromkeys(inflow_names + outflow_names))


def summarize_board_net_values(df: Any, board_names: list[str]) -> dict[str, float]:
    board_map = {
        str(row.板块名称): round(float(row.主力净流入) / 100000000, 2)
        for row in df.itertuples(index=False)
    }
    return {
        board_name: board_map[board_name]
        for board_name in board_names
        if board_name in board_map
    }


def build_market_time_labels() -> list[str]:
    start_hour, start_minute = [int(part) for part in MARKET_OPEN_TIME.split(":")]
    end_hour, end_minute = [int(part) for part in MARKET_CLOSE_TIME.split(":")]
    start_total = start_hour * 60 + start_minute
    end_total = end_hour * 60 + end_minute
    return [
        f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"
        for total_minutes in range(start_total, end_total + 1, TREND_BUCKET_MINUTES)
    ]


def market_time_value(text: str) -> dt_time:
    hour, minute = [int(part) for part in text.split(":")]
    return dt_time(hour=hour, minute=minute)


def market_session_bounds(target_date: date) -> tuple[datetime, datetime]:
    return (
        datetime.combine(target_date, market_time_value(MARKET_OPEN_TIME)),
        datetime.combine(target_date, market_time_value(MARKET_CLOSE_TIME)),
    )


def parse_bucket_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def normalize_session_points(
    points: list[dict[str, Any]],
    target_date: date,
) -> list[dict[str, Any]]:
    session_start, session_end = market_session_bounds(target_date)
    normalized_points: list[dict[str, Any]] = []
    for item in points:
        bucket_dt = parse_bucket_datetime(item.get("bucket"))
        if bucket_dt is None:
            continue
        if not (session_start <= bucket_dt <= session_end):
            continue
        normalized_points.append(
            {
                "bucket": bucket_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "time": bucket_dt.strftime("%H:%M"),
                "boards": item.get("boards", {}),
            }
        )
    return normalized_points


def is_after_market_close(moment: datetime) -> bool:
    _, session_end = market_session_bounds(moment.date())
    return moment > session_end


def build_close_snapshot_point_from_payload(
    payload: dict[str, Any],
    target_date: date,
) -> dict[str, Any] | None:
    boards: dict[str, float] = {}
    for item in list(payload.get("inflow", [])) + list(payload.get("outflow", [])):
        if not isinstance(item, dict):
            continue
        board_name = item.get("name")
        net_value = item.get("net_inflow_yi")
        if not board_name or net_value is None:
            continue
        try:
            boards[str(board_name)] = round(float(net_value), 2)
        except (TypeError, ValueError):
            continue
    if not boards:
        return None

    _, session_end = market_session_bounds(target_date)
    return {
        "bucket": session_end.strftime("%Y-%m-%d %H:%M:%S"),
        "time": session_end.strftime("%H:%M"),
        "boards": boards,
    }


def build_trend_point(
    df: Any,
    captured_at: datetime,
    board_names: list[str],
) -> dict[str, Any]:
    bucket_seconds = TREND_BUCKET_MINUTES * 60
    bucket_epoch = int(captured_at.timestamp()) // bucket_seconds * bucket_seconds
    bucket_dt = datetime.fromtimestamp(bucket_epoch)
    session_start, session_end = market_session_bounds(captured_at.date())
    if bucket_dt < session_start:
        bucket_dt = session_start
    if captured_at > session_end and bucket_dt > session_end:
        bucket_dt = session_end
    return {
        "bucket": bucket_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "time": bucket_dt.strftime("%H:%M"),
        "boards": summarize_board_net_values(df, board_names),
    }


def build_trend_series_from_points(
    points: list[dict[str, Any]],
    board_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    times = build_market_time_labels()
    latest_boards = points[-1]["boards"] if points else {}
    if board_names is None:
        board_names = list(latest_boards.keys())
        if not board_names:
            discovered: list[str] = []
            for point in points:
                for board_name in point.get("boards", {}):
                    if board_name not in discovered:
                        discovered.append(board_name)
            board_names = discovered

    series = []
    for board_name in board_names:
        values_by_time = {
            str(point["time"]): (
                None
                if point["boards"].get(board_name) is None
                else round(float(point["boards"][board_name]), 2)
            )
            for point in points
        }
        values = [values_by_time.get(time_label) for time_label in times]
        if not any(value is not None for value in values):
            continue
        latest_value = next((value for value in reversed(values) if value is not None), None)
        series.append(
            {
                "name": board_name,
                "values": values,
                "latest_value_yi": latest_value,
            }
        )

    return series


def compute_speed_ranking(trend: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    times = trend.get("times", [])
    series = trend.get("series", [])
    if not isinstance(times, list) or not isinstance(series, list):
        return []

    ranking: list[dict[str, Any]] = []
    for item in series:
        if not isinstance(item, dict):
            continue
        values = item.get("values")
        if not isinstance(values, list) or len(values) != len(times):
            continue
        valid_points = [
            (index, float(value))
            for index, value in enumerate(values)
            if isinstance(value, (int, float))
        ]
        if len(valid_points) < 2:
            continue
        (prev_index, prev_value), (last_index, last_value) = valid_points[-2], valid_points[-1]
        interval_minutes = (last_index - prev_index) * TREND_BUCKET_MINUTES
        if interval_minutes <= 0:
            continue
        delta_yi = round(last_value - prev_value, 2)
        speed_yi_per_min = round(delta_yi / interval_minutes, 4)
        if speed_yi_per_min <= 0:
            continue
        ranking.append(
            {
                "name": str(item.get("name", "-")),
                "delta_yi": delta_yi,
                "speed_yi_per_min": speed_yi_per_min,
                "from_time": str(times[prev_index]),
                "to_time": str(times[last_index]),
            }
        )

    ranking.sort(key=lambda row: (row["speed_yi_per_min"], row["delta_yi"]), reverse=True)
    return ranking[:limit]


def build_trend_payload(
    df: Any,
    existing_points: list[dict[str, Any]] | None,
    captured_at: datetime,
    board_names: list[str],
) -> dict[str, Any]:
    points = [
        item
        for item in (existing_points or [])
        if isinstance(item, dict)
        and "bucket" in item
        and "time" in item
        and isinstance(item.get("boards"), dict)
    ]
    points = normalize_session_points(points, captured_at.date())
    point = build_trend_point(df, captured_at, board_names)
    if points and points[-1]["bucket"] == point["bucket"]:
        points[-1] = point
    else:
        points.append(point)
    points = points[-MAX_TREND_POINTS:]
    times = build_market_time_labels()
    series = build_trend_series_from_points(points, board_names)

    return {
        "interval_minutes": TREND_BUCKET_MINUTES,
        "unit": "亿",
        "times": times,
        "series": series,
        "points": points,
    }


def normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    trend = normalized.get("trend")
    if not isinstance(trend, dict):
        trend = {}
    raw_points = trend.get("points")
    points = []
    if isinstance(raw_points, list):
        points = [
            item
            for item in raw_points
            if isinstance(item, dict)
            and "bucket" in item
            and "time" in item
            and isinstance(item.get("boards"), dict)
        ]
    today = datetime.now().date()
    points = normalize_session_points(points, today)
    if not points and is_after_market_close(datetime.now()):
        close_point = build_close_snapshot_point_from_payload(normalized, today)
        if close_point is not None:
            points = [close_point]
    expected_times = build_market_time_labels()
    times = trend.get("times")
    if not isinstance(times, list) or times != expected_times:
        times = expected_times
    series = trend.get("series")
    if (
        not isinstance(series, list)
        or any(not isinstance(item, dict) for item in series)
        or any(len(item.get("values", [])) != len(times) for item in series if isinstance(item, dict))
    ):
        series = build_trend_series_from_points(points)
    speed_ranking = normalized.get("speed_ranking")
    if not isinstance(speed_ranking, list):
        speed_ranking = compute_speed_ranking(
            {
                "times": times,
                "series": series,
            }
        )
    normalized["trend"] = {
        "interval_minutes": trend.get("interval_minutes", TREND_BUCKET_MINUTES),
        "unit": trend.get("unit", "亿"),
        "times": times,
        "series": series,
        "points": points,
    }
    normalized["speed_ranking"] = speed_ranking
    return normalized


def make_payload(
    df: Any,
    requested_indicator_code: str,
    requested_indicator: str,
    actual_indicator: str,
    sector_type: str,
    top: int,
    provider: str,
    fetched_at: datetime,
    recent_3day_map: dict[str, float] | None = None,
    board_detail_map: dict[str, dict[str, str]] | None = None,
    trend: dict[str, Any] | None = None,
    indicator_note: str | None = None,
) -> dict[str, Any]:
    inflow_top = df.sort_values("主力净流入", ascending=False).head(top)
    outflow_top = df.sort_values("主力净流入", ascending=True).head(top)
    recent_3day_map = recent_3day_map or {}
    board_detail_map = board_detail_map or {}

    def build_items(frame: Any) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for row in frame.itertuples(index=False):
            recent_3day_net = recent_3day_map.get(str(row.板块名称))
            board_detail = board_detail_map.get(
                str(row.板块名称),
                {
                    "leader_1": "-",
                    "leader_2": "-",
                    "watch_stock_1": "-",
                    "watch_stock_2": "-",
                },
            )
            items.append(
                {
                    "name": row.板块名称,
                    "net_inflow": float(row.主力净流入),
                    "net_inflow_yi": round(float(row.主力净流入) / 100000000, 2),
                    "change_pct": "-" if row.涨跌幅 != row.涨跌幅 else f"{float(row.涨跌幅):.2f}",
                    "net_ratio": format_ratio(row.主力净流入占比),
                    "recent_3day_net_yi": (
                        None
                        if recent_3day_net is None
                        else round(float(recent_3day_net) / 100000000, 2)
                    ),
                    "leader_1": board_detail["leader_1"],
                    "leader_2": board_detail["leader_2"],
                    "watch_stock_1": board_detail["watch_stock_1"],
                    "watch_stock_2": board_detail["watch_stock_2"],
                }
            )
        return items

    return {
        "requested_indicator_code": requested_indicator_code,
        "requested_indicator": requested_indicator,
        "actual_indicator": actual_indicator,
        "indicator_note": indicator_note,
        "provider": provider,
        "sector_type": sector_type,
        "top": top,
        "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M:%S"),
        "inflow": build_items(inflow_top),
        "outflow": build_items(outflow_top),
        "trend": trend
        or {
            "interval_minutes": TREND_BUCKET_MINUTES,
            "unit": "亿",
            "times": [],
            "series": [],
            "points": [],
        },
        "speed_ranking": compute_speed_ranking(trend or {"times": [], "series": []}),
    }


class FundFlowService:
    def __init__(self, provider: str, indicator: str, sector_type: str, top: int, cache_file: str):
        self.provider = provider
        self.indicator = indicator
        self.sector_type = sector_type
        self.top = top
        self.cache_path = Path(cache_file).resolve()
        self.cache: dict[str, Any] = self._load_cache()
        self._lock = threading.Lock()
        self.board_detail_cache: dict[str, dict[str, dict[str, Any]]] = {
            "行业资金流": {},
            "概念资金流": {},
            "地域资金流": {},
        }

    def _cache_key(self, requested_indicator_code: str) -> str:
        return f"{self.provider}|{requested_indicator_code}|{self.sector_type}|{self.top}"

    def _load_cache(self) -> dict[str, Any]:
        if not self.cache_path.exists():
            return {}
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if isinstance(data, dict) and "inflow" in data and "outflow" in data:
            key = self._cache_key(data.get("requested_indicator_code", "today"))
            return {key: data}
        return data if isinstance(data, dict) else {}

    def _save_cache(self) -> None:
        self.cache_path.write_text(
            json.dumps(self.cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _adapt_cached_payload(self, payload: dict[str, Any], requested_indicator: str) -> dict[str, Any]:
        normalized = normalize_payload(payload)
        normalized["requested_indicator"] = requested_indicator
        normalized["requested_indicator_code"] = period_code_to_indicator_code(requested_indicator)
        normalized["top"] = self.top
        normalized["inflow"] = normalized.get("inflow", [])[: self.top]
        normalized["outflow"] = normalized.get("outflow", [])[: self.top]
        return normalized

    def _find_cached_payload(
        self,
        requested_indicator: str,
        requested_indicator_code: str,
    ) -> dict[str, Any] | None:
        exact = self.cache.get(self._cache_key(requested_indicator_code))
        if isinstance(exact, dict):
            return self._adapt_cached_payload(exact, requested_indicator)

        for payload in self.cache.values():
            if not isinstance(payload, dict):
                continue
            if payload.get("sector_type") != self.sector_type:
                continue
            payload_code = payload.get("requested_indicator_code")
            payload_indicator = payload.get("requested_indicator")
            if payload_code == requested_indicator_code or payload_indicator == requested_indicator:
                return self._adapt_cached_payload(payload, requested_indicator)
        return None

    def get_payload(self, requested_indicator: str | None = None, requested_indicator_code: str | None = None) -> dict[str, Any]:
        with self._lock:
            requested_indicator = requested_indicator or self.indicator
            requested_indicator_code = requested_indicator_code or "today"
            actual_indicator, indicator_note = resolve_requested_indicator(requested_indicator)
            cache_key = self._cache_key(requested_indicator_code)
            try:
                captured_at = datetime.now()
                df, actual_provider = fetch_sector_fund_flow(
                    actual_indicator,
                    self.sector_type,
                    provider=self.provider,
                )
                trend_board_names = select_trend_boards(df)
                trend = build_trend_payload(
                    df,
                    self.cache.get(cache_key, {}).get("trend", {}).get("points"),
                    captured_at,
                    trend_board_names,
                )
                board_names = list(
                    dict.fromkeys(
                        df.sort_values("主力净流入", ascending=False).head(self.top)["板块名称"].astype(str).tolist()
                        + df.sort_values("主力净流入", ascending=True).head(self.top)["板块名称"].astype(str).tolist()
                    )
                )
                board_detail_map = build_board_detail_map(
                    self.sector_type,
                    board_names,
                    self.board_detail_cache[self.sector_type],
                )
                payload = make_payload(
                    df,
                    requested_indicator_code,
                    requested_indicator,
                    actual_indicator,
                    self.sector_type,
                    self.top,
                    actual_provider,
                    captured_at,
                    build_recent_3day_map(self.sector_type, actual_provider),
                    board_detail_map,
                    trend,
                    indicator_note,
                )
                payload["source"] = "live"
                payload["error"] = None
                payload = normalize_payload(payload)
                self.cache[cache_key] = payload
                self._save_cache()
                return payload
            except Exception as exc:
                payload = self._find_cached_payload(requested_indicator, requested_indicator_code)
                if payload is not None:
                    payload["source"] = "cache"
                    payload["error"] = summarize_fetch_error(exc)
                    return payload
                raise RuntimeError(f"实时拉取失败，且本地没有可用快照: {exc}") from exc


class RequestHandler(BaseHTTPRequestHandler):
    service: FundFlowService

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/":
            self._send_html(HTML_PAGE)
            return
        if path == "/api/data":
            self._handle_api(parsed.query)
            return
        if path == "/healthz":
            self._send_json({"status": "ok"})
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _handle_api(self, query: str) -> None:
        params = parse_qs(query)
        period_code = params.get("period", [None])[0]
        indicator = period_code_to_indicator(period_code)
        try:
            payload = self.service.get_payload(
                requested_indicator=indicator,
                requested_indicator_code=period_code,
            )
        except Exception as exc:
            self._send_json(
                {"error": str(exc)},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._send_json(payload)

    def _send_html(self, html: str) -> None:
        content = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def main() -> int:
    args = parse_args()
    if args.top <= 0:
        raise SystemExit("--top 必须大于 0")

    disable_system_proxy()
    service = FundFlowService(args.provider, args.indicator, args.sector_type, args.top, args.cache_file)
    RequestHandler.service = service

    server = HTTPServer((args.host, args.port), RequestHandler)
    print(f"Web 服务已启动: http://{args.host}:{args.port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
