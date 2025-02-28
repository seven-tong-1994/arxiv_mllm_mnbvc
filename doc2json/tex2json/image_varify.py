import io
from typing import Tuple

from loguru import logger
from arxiv_to_mm import ArxivBlock
from pathlib import Path
import pandas as pd
from PIL import Image as PILImage


def img_to_bytes(img_path: Path) -> Tuple[bytes, Tuple[int, int]]:
    """将图片文件转换为二进制格式

    Args:
        img_path: 图片文件路径

    Returns:
        bytes: 图片文件的二进制数据
        Tuple[int, int]: 图片的宽度和高度
    """
    try:
        with open(img_path, 'rb') as file:
            image = PILImage.open(file)
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format=image.format)
            img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr, image.size
    except Exception as e:
        logger.error(f"图片转换二进制失败: {e}")
        return None


def bytes_to_img(img_byte_arr, img_path):
    """将二进制数据转换为图片并保存

    Args:
        img_byte_arr: 图片的二进制数据
        img_path: 保存图片的路径
    """
    # try:
    image = PILImage.open(io.BytesIO(img_byte_arr))
    image.save(img_path)
    logger.info(f"图片已保存到: {img_path}")
    # except Exception as e:
    #     logger.error(f"二进制数据转换图片失败: {e}")


def main():
    parquet_file = Path("test_0.parquet")
    df = pd.read_parquet(parquet_file)
    # df to dict
    rows = df.to_dict(orient="records")
    # df to blocks
    for row in rows:
        block = ArxivBlock()
        block.from_dict(row)

        if block.category == 'figure':
            bytes_to_img(block.image_data.encode('utf-8'), Path(f"outputs/demo_{block.block_id}.png"))


if __name__ == "__main__":
    main()