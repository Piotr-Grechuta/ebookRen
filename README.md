# ebookRen

Windows-focused ebook renamer/copy-archiver with GUI and optional online metadata enrichment.

## Features

- Renames ebook files in place or creates renamed copies in a selected destination folder
- Optionally moves original source files to a separate archive folder after the renamed copy is created
- Supports `epub`, `mobi`, `azw`, `azw3`, `pdf`, `lit`, `fb2`, `rtf`, and `txt`
- GUI only
- GUI includes three tabs:
  - renamer / copy / archive workflow
  - embedded metadata backfill for already renamed files
  - export/conversion of non-EPUB books to EPUB with calibre
- Preview, apply, destination-copy mode, archive-originals mode, and undo from CSV log exported by GUI
- Optional online enrichment from public catalog providers
- Optional embedded metadata writing during `Apply`
  - `epub`: native in-file update
  - other writable formats: via calibre `ebook-meta` when available
- Optional EPUB export workflow via calibre `ebook-convert`
  - if a matching source `*.epub` already exists, it is moved to destination instead of converting again
  - sibling non-EPUB duplicates with the same basename can be discarded
- Direct embedded metadata parsing is currently implemented for `epub`; other supported extensions rely on filename heuristics and optional online lookup

## Filename pattern

The tool generates names in this form:

`Author - Series - Tom XX.YY - Title.ext`

When the tool can infer a broad genre from EPUB subjects or online catalog metadata, it appends it to the title:

`Author - Series - Tom XX.YY - Title [genre].ext`

## Format support

- `epub`: reads embedded metadata (`title`, `creator`, `identifier`, `subject`, selected series fields)
- `epub`: can also write embedded metadata on `Apply` for easier import into Calibre
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

Hurtowe uzupelnienie metadanych juz przemianowanych plikow:

```powershell
python backfill_embedded_metadata.py D:\Sciezka\Do\Biblioteki --recursive --apply --killim
```

W GUI:

- `Renamer`: dotychczasowy tryb zmiany nazw / kopii / archiwum
- `Metadane`: hurtowe uzupelnienie osadzonych metadanych na podstawie wzorca nazwy pliku; pole tagow jest domyslnie wypelnione `Killim`
- `Konwersja EPUB`: eksportuje ksiazki do `epub` przez calibre, potrafi przeniesc juz istniejace `epub` do folderu docelowego i usuwa zrodlowe duplikaty tej samej nazwy bazowej, gdy `epub` juz istnieje
