import json
import re

placeholder = r'nolistsep'


def get_abstract(dict_data, title, authors):
    abstract_texts = []
    for abstract in dict_data:
        if "text" in abstract:
            abstract_text = re.sub(placeholder, '', abstract['text'].strip())
            abstract_texts.append(abstract_text)
    abstract_full = "\n".join(abstract_texts).strip()
    if len(title) == 0 and len(authors) == 0:
        preffix = abstract_full.split('\n\n')[0].splitlines()
        abstract_full = '\n\n'.join(abstract_full.split('\n\n')[1:])
        title = ''
        authors = ''
        if len(preffix) > 0:
            title = preffix[0]
            authors = '\n'.join(preffix[1:])
    return abstract_full, title, authors


def get_body(dict_data, is_format=True):
    title_cached = {}
    body_text = []
    for paragraph in dict_data:
        paragraph_title = paragraph.get("section", "").strip()
        paragraph_text = paragraph.get("text", "").strip()
        if paragraph_title:
            title_list = paragraph_title.split('::')
            for level, title in enumerate(title_list):
                if not title_cached.get(title):
                    title_cached[title] = level + 1
                    if is_format:
                        body_text.append(f"[SECTION_PLACEHOLDER]{'#' * title_cached[title]} {title}[\SECTION_PLACEHOLDER]")
                    else:
                        body_text.append(title)
            body_text.append(paragraph_text)

        else:
            body_text.append(paragraph_text)

    return '\n\n'.join(body_text)


def normal_bibgraphy(dict_data, text):
    index = 1
    references = {}
    for _, bib_entry in dict_data.items():
        ref_string = format_bibgraphy(bib_entry)
        ref_id = bib_entry['ref_id']
        text = re.sub(ref_id, f'[{index}]', text, flags=re.DOTALL)
        references[index] = ref_string
        index += 1
    return text, references


def format_bibgraphy(reference):
    title = reference.get("title", "Untitled")
    authors = reference.get("authors", [])
    year = reference.get("year", "n.d.")  # "n.d." 表示无年份
    venue = reference.get("venue", "")
    volume = reference.get("volume", "")
    issue = reference.get("issue", "")
    pages = reference.get("pages", "")
    urls = reference.get("urls", [])
    raw_text = reference.get("raw_text", "")
    if raw_text:
        formatted_reference = raw_text
    else:
        if authors:
            name_list = []
            for author in authors:
                name = [author['first']]
                name.append(' '.join(author['middle'])) if len(author['middle']) > 0 else name
                name.append(author['last'])
                name_list.append(' '.join(name))

            formatted_authors = ", ".join(name_list[:-1]) + (f" & {name_list[-1]}" if len(name_list) > 1 else name_list[0])
        else:
            formatted_authors = "Unknown Author"

        journal_info = ""
        if venue:
            journal_info += f"{venue}"
        if volume:
            journal_info += f", {volume}"
        if issue:
            journal_info += f"({issue})"
        if pages:
            journal_info += f", pp. {pages}"

        url = urls[0] if urls else ""
        formatted_reference = f"{formatted_authors} ({year}). *{title}*. {journal_info.strip()}."
        if url:
            formatted_reference += f" Retrieved from {url}"
    return formatted_reference




def normal_reference(dict_data, text):
    # figure_index = 1
    footnote = {}

    for ref_id, ref_entry in dict_data.items():
        if ref_id.startswith('FIGREF'):  # 图像处理
            figure_index = ref_entry['num']

            img_lists = [f'\n\n[BEGIN_FIGURE_PLACEHOLDER]{json.dumps(ref_entry, ensure_ascii=False)}[END_FIGURE_PLACEHOLDER]\n\n']
            img_string = '\n\n'.join(img_lists)
            line_index = text.find(ref_id)
            text = text.replace(f' {ref_id} ', f' {figure_index} ')
            if line_index != -1:
                textlines = text.split('\n\n')
                insert_position = len(text[:line_index].split('\n\n')) - 1

                textlines.insert(insert_position, img_string)
                text = '\n\n'.join(textlines)
            else:
                text += '\n\n' + img_string
        elif ref_id.startswith('TABREF'):  # 表格处理
            table_num = ref_entry['num']
            html = ref_entry['html']
            caption = ref_entry['text']
            table_string = caption + '\n\n' + html

            line_index = text.find(ref_id)
            text = text.replace(f' {ref_id} ', f' {table_num} ')
            if line_index != -1:
                textlines = text.split('\n\n')
                insert_position = len(text[:line_index].split('\n\n')) - 1
                textlines.insert(insert_position, table_string)
                text = '\n\n'.join(textlines)
            else:
                text += '\n\n' + table_string
        elif ref_id.startswith('FOOTREF'):  # 脚注处理
            footnote_text = ref_entry['text']
            footnote_num = ref_entry['num']
            text = text.replace(f' {ref_id} ', f'[^{footnote_num}]')
            footnote[footnote_num] = footnote_text
        elif ref_id.startswith('SECREF'):  # section 处理
            parent = ref_entry['parent']
            section_num = [ref_entry['num']]
            while parent:
                section_num.append(dict_data[parent]["num"])
                parent = dict_data[parent]['parent']
            if None not in section_num:
                section_name = ".".join(section_num[::-1])
                text = text.replace(f' {ref_id} ', f' {section_name} ')
            else:
                text = text.replace(f' {ref_id} ', f' ')
    return text, footnote

def addition_reference(text, bibgraphy, footnote):
    footnote_list = []
    for index, footnote_text in footnote.items():
        footnote_list.append(f'[{index}] {footnote_text}')
    footnote_string = '[FOOT_PLACEHOLDEF]' + '\n'.join(footnote_list) + '[\FOOT_PLACEHOLDEF]'

    text += f'\n\n# FootNote\n\n{footnote_string}'

    bib_list = []
    for index, bib_text in bibgraphy.items():
        bib_list.append(f'[{index}] {bib_text}')
    bib_string = '[REFERENCE_PLACEHOLDEF]' + '\n'.join(bib_list) + '[\REFERENCE_PLACEHOLDEF]'
    text += f'\n\n# Reference\n\n{bib_string}'
    return text


def convert_json_to_markdown(json_data):
    title = json_data.get("title", "").strip()
    authors = json_data.get("authors", [])
    abstract, title, authors = get_abstract(json_data['latex_parse']['abstract'], title, authors)
    body_text = get_body(json_data['latex_parse']['body_text'], is_format=True)  # 是否保留标题的 ## 结构
    text = f'** {title} **\n\n'
    text += f'{authors} \n\n'
    text += f'\n\n{abstract} \n\n'
    # text += f'Abstract \n\n {abstract} \n\n'
    text += body_text
    text, bibgraphy = normal_bibgraphy(json_data['latex_parse']['bib_entries'], text)
    text, footnote = normal_reference(json_data['latex_parse']['ref_entries'], text)
    text = addition_reference(text, bibgraphy, footnote)
    return text


if __name__ == '__main__':
    # with open("../../output_dir/2004.14974.json", "r", encoding='utf-8') as f:
    #     data = json.load(f)
    # md_data = convert_json_to_markdown(data)
    # fw = open('../../output_dir/2004.14974.md', 'w', encoding='utf-8')
    # fw.write(md_data)
    pdf_path = ''








