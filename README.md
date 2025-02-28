# Arxiv文档解析(LaTex->Parquet)

## 安装环境

```
conda create -n doc2json python=3.8 pytest
conda activate doc2json
pip install -r requirements.txt
apt install texlive-extra-utils tralics
python setup.py develop
```

## 安装Grobid
```
bash scripts/setup_grobid.sh
bash scripts/run_grobid.sh
```
ps:安装结束后，运行grobid的时候，进度条可能卡在91%，这个是正常状态

## 开始解析
将LaTex压缩包放入/test/latex中，设置输出文件夹，首先启动Groid,运行代码。代码运行例子如下：
```
cd s2orc-doc2json
bash scripts/run_grobid.sh
python doc2json/tex2json/process_tex.py -i test/latex/2004.14974.gz -t temp_dir/ -o output_dir/
```
结果可以在output_dir查看，其中输入文件名的json文件是使用Grobid解析的结果，parquet文件为最终结果。

```
./output_dir/2004.14974.json  为使用 Grobid 解析的结果
./output_dir/2004.14974.parquet  为由 mnbvc 多模态结构组层的结果
```

## parquet文件结构如下
```
"文件md5": str,   # 文件名的md5值
"文件id": str,  # arxiv id
"页码": None,  # pdf 页码，为None，未分页
"块id": int,  # parquet块id，由0开始逐渐递增
"文本": str,  # 文本内容
"图像": byte,  # 图像的二进制格式，如果有 
"块类型": str,  # 类型，分文本（text），图像（figure），表格（table）
"处理时间": str,  # 当前的时间戳
"元数据": str,  # 当前 parquet 块的补充信息，包含如下三个字段 {"text_length": , "type": "", "image_size": {}}


```
## json中提取信息的方法：
```
def read_parquet(path):
    # 从 Parquet 文件读取数据
    df = pd.read_parquet(path)
    # df to dict
    rows = df.to_dict(orient="records")
    # df to blocks
    for row in rows:
        block = ArxivBlock()
        block.from_dict(row)
        # 此时 block 数据与parquet结构相同；当block的类型为图像时，可将其转化为图片保存
        if block.image_data:
            bytes_to_img(block.image_data, Path(f"demo_{block.block_id}.png"))
            
        
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

```