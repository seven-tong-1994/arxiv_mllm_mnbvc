import base64
from datetime import datetime
import re
import hashlib
from typing import Dict, List, Tuple
import json
import io
from typing import Tuple
from loguru import logger
from PIL import Image as PILImage
from pdf2image import convert_from_path
from pathlib import Path
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

from doc2json.tex2json.json_to_md import convert_json_to_markdown


class ArxivBlock:
    def __init__(self, **kwargs) -> None:
        self.file_md5 = kwargs.get('file_md5')  # 图片md5 / json 内容 md5
        self.file_id = kwargs.get('file_id', 'default_file_id')  # arxiv 源的 pdf id
        self.block_id = kwargs.get('block_id')  # 内部生成的 id
        self.text = kwargs.get('text')  # 每行内容，文本、表格为html
        self.image_data = kwargs.get('image_data')  # 图像内容
        self.category = kwargs.get('category')  # text、image、table
        self.timestamp = kwargs.get('timestamp')  # 处理时间戳
        self.meta_data = kwargs.get('meta_data')  # 元类型

    def to_dict(self) -> Dict:
        return {
            "文件md5": str(self.file_md5),
            "文件id": str(self.file_id),
            "页码": None,
            "块id": int(self.block_id),
            "文本": str(self.text),
            "图像": self.image_data,
            "块类型": str(self.category),
            "处理时间": str(self.timestamp),
            "元数据": str(self.meta_data)
        }

    def from_dict(self, dict_data: Dict):
        self.file_md5 = dict_data.get('文件md5')
        self.file_id = dict_data.get('文件id')
        self.block_id = dict_data.get('块id')
        self.text = dict_data.get('文本')
        self.image_data = dict_data.get('图像')
        self.category = dict_data.get('块类型')
        self.timestamp = dict_data.get('处理时间')
        self.meta_data = dict_data.get('元数据')

    def to_json(self) -> str:
        dict_data = self.to_dict()
        dict_data["图片"] = base64.b64encode(dict_data['图片']).decode('utf-8')
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def __repr__(self) -> str:
        return rf"""
        =块id: {self.block_id:04}=
        文件id: {self.file_id},  块id: {self.block_id}, 处理时间: {self.timestamp} \
        文本或图像: {self.text[:100] if self.text != None else str(self.image_data)[:100]}\
        ======
            """

def get_timestamp():
    return datetime.now().strftime("%Y%m%d")


def format_figure(figure_content, is_md_format=False):
    figure_data = json.loads(figure_content)
    uris = figure_data['uris']
    figure_index = figure_data['num']
    caption = f'Figure {figure_index}: {figure_data["text"]}'

    binary_list = []
    size_list = []
    if is_md_format:
        caption = f'![Figure {figure_index}: {figure_data["text"]}]'
    if len(uris) == 1:
        image_binary, img_size = read_image(uris[0])
        binary_list.append(image_binary)

        size_list.append({'text_length': 0, 'type': 'figure', "image_size": {
                "width": img_size[0],
                "height": img_size[1],
            }})
    else:
        for sub_img_uri in uris:
            image_binary, img_size = read_image(sub_img_uri)
            binary_list.append(image_binary)
            size_list.append({"image_size": {
                "width": img_size[0],
                "height": img_size[1],
            }})

    return caption, binary_list, size_list


def read_image(img_path: Path) -> Tuple[bytes, Tuple[int, int]]:
    """将图片文件转换为二进制格式

    Args:
        img_path: 图片文件路径

    Returns:
        bytes: 图片文件的二进制数据
        Tuple[int, int]: 图片的宽度和高度
    """
    try:
        print(img_path)
        if Path(img_path).suffix.lower() == ".pdf":
            image = convert_from_path(img_path)[0]
        else:
            with open(img_path, 'rb') as file:
                image = PILImage.open(file)
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format=image.format)
        img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr, image.size
    except Exception as e:
        logger.error(f"图片转换二进制失败: {e}")
        return None, (0, 0)



def convert_to_rows(input_file: Path):
    rows = []
    json_file_md5 = hashlib.md5(input_file.read_bytes()).hexdigest()
    json_name = input_file.name
    block_id = 0

    with open(input_file, 'r') as file:
        data = json.load(file)

    md_data = convert_json_to_markdown(data)
    for paragraph in md_data.split('\n\n'):
        if len(paragraph.strip()) == 0:
            continue

        figure_match = re.search(r'^\[BEGIN_FIGURE_PLACEHOLDER](.*?)\[END_FIGURE_PLACEHOLDER]', paragraph)
        table_match = re.search(r'^<table>(.*?)</table>$', paragraph)
        if figure_match:  # 图像
            figure_content = figure_match.group(1)
            caption, binary_list, size_list = format_figure(figure_content)
            text_content = [caption]
            image_content = [None]
            category = ['text']
            meta_data = [{'text_length': len(caption), 'type': 'text', 'image_size': {}}]
            text_content.extend([None] * len(binary_list))
            category.extend(['figure'] * len(binary_list))
            image_content.extend(binary_list)
            meta_data.extend(size_list)

        elif table_match:
            text_content = [table_match.group()]
            image_content = [None]
            category = ['table']
            meta_data = [{'text_length': len(table_match.group()), 'type': 'table', 'image_size': {}}]
        else:
            text_content = []
            meta_data = []
            section_match = re.search(r'\[SECTION_PLACEHOLDER](.*?)\[\\SECTION_PLACEHOLDER]', paragraph, re.DOTALL)
            ref_match = re.search(r'\[REFERENCE_PLACEHOLDEF](.*?)\[\\REFERENCE_PLACEHOLDEF]', paragraph, re.DOTALL)
            footnote_match = re.search(r'\[FOOT_PLACEHOLDEF](.*?)\[\\FOOT_PLACEHOLDEF]', paragraph, re.DOTALL)
            if section_match:
                text_content = [section_match.group(1)]
                meta_data = [{'text_length': len(section_match.group(1)), 'type': 'section title', 'image_size': {}}]
            elif ref_match:
                text_content = [ref_match.group(1)]
                meta_data = [{'text_length': len(ref_match.group(1)), 'type': 'reference', 'image_size': {}}]
            elif footnote_match:
                text_content = [footnote_match.group(1)]
                meta_data = [{'text_length': len(footnote_match.group(1)), 'type': 'footnote', 'image_size': {}}]
            elif len(paragraph) > 0:
                text_content = [paragraph]
                meta_data = [{'text_length': len(paragraph), 'type': 'text', 'image_size': {}}]
            image_content = [None]
            category = ['text']

        for i in range(len(text_content)):
            text = text_content[i]
            image = image_content[i]
            item_category = category[i]
            meta = meta_data[i]
            rows.append(ArxivBlock(
                file_md5=json_file_md5,
                file_id=str(json_name).replace('.json', ''),
                block_id=block_id,
                text=text,
                image_data=image,
                category=item_category,
                timestamp=get_timestamp(),
                meta_data=json.dumps(meta, ensure_ascii=False),
            ))
            block_id += 1
    logger.info(
        f"process {input_file} done, {len(rows)} rows generated, {json_file_md5} {json_name}")
    return rows


def batch_to_parquet(output_file: Path, split_size: int, batchs: List[ArxivBlock]):
    # 将 rows 写入 parquet 文件
    batch_rows = []
    # 将 batchs 按 split_size 分割，
    # 当 batch 长度大于 split_size 时，将 batch 写入 parquet 文件
    # 当 batch 长度小于 split_size 时，继续追加 batch_rows
    batch_count = 0
    split_count = 0
    for batch in batchs:
        batch_count += 1
        batch_rows.append(batch)
        if batch_count >= split_size:
            df = pd.DataFrame([row.to_dict() for row in batch_rows])
            output_file_split = output_file.parent / \
                f"{output_file.stem}_{split_count}.parquet"
            # 使用 pyarrow 引擎
            table = pa.Table.from_pandas(df)
            # 保存为 parquet
            parquet.write_table(table, output_file_split)
            logger.info(
                f"batch {split_count} done, {output_file_split} generated")
            batch_rows = []
            batch_count = 0
            split_count += 1

    # 处理最后一个 batch
    if batch_rows:
        df = pd.DataFrame([row.to_dict() for row in batch_rows])
        output_file_last = output_file.parent / \
            f"{output_file.stem}_{split_count}.parquet"
        table = pa.Table.from_pandas(df)
        parquet.write_table(table, output_file_last)
        logger.info(f"batch {split_count} done, {output_file_last} generated")

def main():
    parser = argparse.ArgumentParser(description="Docling Convert")
    parser.add_argument("--input_file", "-i", type=Path, help="Input file")
    parser.add_argument("--output_file", "-o", type=Path, help="Output file")
    parser.add_argument("--split_size", "-s", type=int, default=200,
                        help="Split size")  # 500-1000MB 一个 parquet 文件
    parser.add_argument("--log_dir", "-l", type=Path,
                        default="logs", help="Log level")
    args = parser.parse_args()

    current_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    input_file = args.input_file
    output_file = args.output_file
    split_size = args.split_size

    log_dir = args.log_dir
    logger_file = log_dir / f"to_mm_{current_date}.log"
    logger.add(logger_file, encoding="utf-8", rotation="500MB")

    # # for paragraph in md_data.split('\n\n'):
    batchs = convert_to_rows(input_file)
    batch_to_parquet(output_file, split_size, batchs)


def bytes_to_img(img_byte_arr, img_path):
    """将二进制数据转换为图片并保存

    Args:
        img_byte_arr: 图片的二进制数据
        img_path: 保存图片的路径
    """
    try:
        image = PILImage.open(io.BytesIO(img_byte_arr))
        image.save(img_path)
        logger.info(f"图片已保存到: {img_path}")
    except Exception as e:
        logger.error(f"二进制数据转换图片失败: {e}")


def read_parquet(path):
    # 从 Parquet 文件读取数据
    df = pd.read_parquet(path)
    # df to dict
    rows = df.to_dict(orient="records")
    # df to blocks
    for row in rows:
        block = ArxivBlock()
        block.from_dict(row)

        if block.image_data:
            bytes_to_img(block.image_data, Path(f"demo_{block.block_id}.png"))


if __name__ == '__main__':
    # main()
    read_parquet('../../output_dir/2004.14974_0.parquet')