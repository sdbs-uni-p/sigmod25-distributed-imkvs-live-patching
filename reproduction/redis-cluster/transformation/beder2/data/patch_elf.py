import os
from typing import List

import pandas as pd
from elftools.elf.descriptions import describe_sh_type
from elftools.elf.elffile import ELFFile


def read_patch_elf(file: List[str]) -> pd.DataFrame:
    df = None
    for f in file:
        df_file = _read(f)

        df_file["file_name"] = os.path.basename(f)

        df = df_file if df is None else pd.concat([df, df_file], ignore_index=True)
    return df


def _read(file: str) -> pd.DataFrame:
    with open(file, "rb") as f:
        elffile = ELFFile(f)

        data = [
            {
                "section_name": sec.name,
                "size_bytes": sec.data_size,
                "type": describe_sh_type(sec["sh_type"]),
            }
            for sec in elffile.iter_sections()
        ]
        return pd.DataFrame(data=data)
