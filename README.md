# ebookRen

Windows-focused ebook renamer/copier with optional online metadata enrichment.

## Features

- Renames or copies ebook files to a normalized filename pattern
- Supports `epub`, `mobi`, `azw`, `azw3`, `pdf`, `lit`, `fb2`, `rtf`, and `txt`
- GUI by default, CLI available with `--cli`
- Preview, apply, destination-copy mode, and undo from CSV log
- Optional online enrichment from public catalog providers
- Direct embedded metadata parsing is currently implemented for `epub`; other supported extensions rely on filename heuristics and optional online lookup

## Filename pattern

The tool generates names in this form:

`Author - Series - Tom XX.YY - Title.ext`

When the tool can infer a broad genre from EPUB subjects or online catalog metadata, it appends it to the title:

`Author - Series - Tom XX.YY - Title [genre].ext`

## Format support

- `epub`: reads embedded metadata (`title`, `creator`, `identifier`, `subject`, selected series fields)
- `mobi`, `azw`, `azw3`, `pdf`, `lit`, `fb2`, `rtf`, `txt`: currently normalized from filename parsing plus optional online enrichment

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
