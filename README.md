# ebookRen

Windows-focused ebook renamer/copier with optional online metadata enrichment.

## Features

- Renames or copies ebook files to a normalized filename pattern
- Supports `epub`, `mobi`, `azw`, `azw3`, `pdf`, `lit`, `fb2`, `rtf`, and `txt`
- GUI by default, CLI available with `--cli`
- Preview, apply, destination-copy mode, and undo from CSV log
- Optional online enrichment from public catalog providers

## Filename pattern

The tool generates names in this form:

`Author - Series - Tom XX.YY - Title.ext`

## Requirements

- Python 3.11+
- `ebooklib`

Install dependencies:

```powershell
pip install -r requirements.txt
```

## Usage

GUI:

```powershell
python ebookRen.py
```

CLI preview:

```powershell
python ebookRen.py --cli --folder g:\books
```

CLI copy with online enrichment:

```powershell
python ebookRen.py --cli --folder g:\books --destination g:\books_out --online --apply
```

Undo from CSV log:

```powershell
python ebookRen.py --cli --undo rename_books_20260308_120000.csv
```
