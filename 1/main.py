import asyncio
from argparse import ArgumentParser
from aioshutil import copyfile, Error
from aiopath import AsyncPath
import logging

async def copy_file(file: AsyncPath, dest: AsyncPath):
    try:
        ext = file.suffix[1:]
        if not ext:
            ext = "no_ext"
        dst_path = dest/ext
        await dst_path.mkdir(parents=True, exist_ok=True)
        await copyfile(file, dst_path/file.name)
    except Error as e:
        logging.error(f"Error copying {file}: {e}")
    except OSError as e:
        logging.error(f"OS error occurred while copying {file}: {e}")
    except Exception as e:
        logging.error("Помилка при операціях з папками/файлами", e)    

async def read_folder(outp_p: AsyncPath, dist_p: AsyncPath):
    try:
        if not await outp_p.exists() or not await outp_p.is_dir():
            logging.error("За вказаним шляхом не знайдено необхідної папки")
            return
        async for src_path in outp_p.iterdir():
            if await src_path.is_dir():
                await read_folder(src_path, dist_p)
            elif await src_path.is_file():
                await copy_file(src_path, dist_p)
            else:
                logging.error(f"Тут {outp_p.name} щось незрозуміле {src_path.name}")
    except Exception as e:
        logging.error("Помилка при операціях з папками/файлами", e)

def main():
    parser = ArgumentParser()
    parser.add_argument("-s", "--source", help="Source folder", default="1")
    parser.add_argument("-d", "--destination", help="Destination folder", default="Result")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    
    source = AsyncPath(args.source)
    destination = AsyncPath(args.destination)

    asyncio.run(read_folder(source, destination))

if __name__ == "__main__":
    main()