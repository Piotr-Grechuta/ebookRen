# ebookRen

Windows-focused ebook renamer/copy-archiver with GUI and optional online metadata enrichment.

## Features

- Renames ebook files in place or creates renamed copies in a selected destination folder
- Optionally moves original source files to a separate archive folder after the renamed copy is created
- Supports `epub`, `mobi`, `azw`, `azw3`, `pdf`, `lit`, `fb2`, `rtf`, and `txt`
- GUI only
- Preview, apply, destination-copy mode, archive-originals mode, and undo from CSV log exported by GUI
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

Uruchomienie:

```powershell
python ebookRen.py
```
