# LMS-MikroTik-LastOnline.py

Prosty skrypt w Pythonie do aktualizacji pola `lastonline` w LMS na podstawie tablicy ARP pobranej z urządzeń MikroTik przez API.

## Co robi

Skrypt:

- łączy się z bazą LMS
- pobiera mapowanie `IP + MAC -> ID`
- łączy się z MikroTikami zdefiniowanymi w `devices.ini`
- pobiera dynamiczne wpisy ARP
- dopasowuje urządzenia do wpisów w LMS
- aktualizuje pole `lastonline` w tabeli `nodes`

## Wymagania

- Python 3.11+  
- dostęp do bazy MySQL lub PostgreSQL
- dostęp do MikroTik API
- binarka `ping`

## Instalacja

### MySQL

```bash
python3 -m pip install pymysql routeros-api
