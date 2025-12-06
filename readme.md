# IDX Shareholder Parser

A desktop GUI tool for fetching and parsing 5%+ shareholder disclosures from the Indonesia Stock Exchange (IDX). The application identifies issuers (emiten) with ownership changes and lists all of their current 5%+ shareholders in a structured table.

<p align="center">
  <img src="https://i.imgur.com/VJWgyfH.png" alt="IDX Shareholder Parser Screenshot" width="550">
  <br>
  <em>Simple view</em>
</p>

<p align="center">
  <img src="https://i.imgur.com/SIFcBrw.png" alt="IDX Shareholder Parser Screenshot" width="900">
  <br>
  <em>Detailed view</em>
</p>

## Run Without Installation (Windows Only)

To use the app, simply **download the latest ZIP release** from the **Releases** section and run the .exe file.

## Features

- Fetch the latest or a specific date IDX disclosure
- Automatically parse IDX PDF files
- Includes only issuers with ownership changes
- Displays all current 5%+ shareholders for each changing issuer
- Automatically translate into CSV

## Environment

- **Python:** 3.13.7
- **Platform:** Windows / Linux / macOS

## Requirements

Install all dependencies using the provided `requirements.txt` file:

```bash
pip install -r requirements.txt
```

## Credits

All credits goes to Hengky Adinata and Remora Trader

## Support My Next Stoploss

Support me — or at least my next losing trade XD

[![Saweria](https://img.shields.io/badge/Donate-Saweria-orange)](https://saweria.co/araafario)
