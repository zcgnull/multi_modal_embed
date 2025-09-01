# app_gradio.py

import gradio as gr
import requests
import uuid
from typing import List, Dict, Any

# 配置后端地址（根据你的 Flask 服务地址修改）
BACKEND_URL = "http://localhost:9090/v1"  # 修改为你的 Flask 服务地址

# 全局缓存知识库列表
def list_kb_tab():
    with gr.Tab("Knowledge Base List"):
        gr.Markdown("## 📚 Knowledge Base List")
        
        # 输出组件
        kb_list_output = gr.Dataframe(
            label="Knowledge Bases",
            headers=["ID", "Name", "Bucket", "Collection", "Model"],
            datatype=["str", "str", "str", "str", "str"],
            interactive=False
        )
        
        # 刷新按钮
        refresh_btn = gr.Button("🔄 Refresh List")
        
        def fetch_kb_list():
            try:
                response = requests.get(f"{BACKEND_URL}/kb/list")  # 根据实际API路径调整
                if response.status_code == 200:
                    result = response.json().get("data", [])
                    # 转换为表格数据格式
                    table_data = []
                    for kb in result:
                        table_data.append([
                            kb.get("kb_id", ""),
                            kb.get("kb_name", ""),
                            kb.get("bucket", ""),
                            kb.get("collection", ""),
                            kb.get("model", "")
                        ])
                    return table_data
                else:
                    gr.Warning(f"Failed to fetch knowledge bases: {response.status_code}")
                    return []
            except Exception as e:
                gr.Error(f"Error fetching knowledge bases: {str(e)}")
                return []
        
        # 绑定按钮点击事件
        refresh_btn.click(
            fn=fetch_kb_list,
            inputs=[],
            outputs=[kb_list_output]
        )
        
        # 页面加载时自动获取一次数据
        demo.load(
            fn=fetch_kb_list,
            inputs=[],
            outputs=[kb_list_output]
        )
    
    return gr.Tab

# 1. 创建知识库 Tab
def create_kb_tab():
    with gr.Tab("Create Knowledge Base"):
        kb_name = gr.Textbox(label="Knowledge Base Name (only letters and digits)", placeholder="mykb123")
        vector_size = gr.Number(label="Vector Size", value=768, precision=0)
        model = gr.Dropdown(["BaaiVl", "Qwen"], label="Model", value="BaaiVl")
        
        output = gr.JSON()
        
        def create_kb(name, vec_size, model_name):
            if not name or not name.isalnum():
                return {"error": "Name must be non-empty and contain only letters and digits."}
            try:
                vec_size = int(vec_size)
            except:
                return {"error": "Vector size must be an integer."}
            
            response = requests.post(
                f"{BACKEND_URL}/kb/create",
                data={
                    "kb_name": name,
                    "vector_size": vec_size,
                    "model": model_name
                }
            )
            if response.status_code == 200:
                result = response.json()
                return result.get("data", result)
            else:
                return {"error": response.json().get("message", "Unknown error")}

        btn = gr.Button("Create Knowledge Base")
        btn.click(create_kb, inputs=[kb_name, vector_size, model], outputs=output)
    
    return gr.Tab

# 2. 插入数据 Tab
def insert_data_tab():
    with gr.Tab("Insert Data into Knowledge Base"):
        kb_id = gr.Textbox(label="Knowledge Base ID", placeholder="Enter KB ID (e.g., 1, 2, ...)")
        text_input = gr.Textbox(label="Optional Text", placeholder="Enter description or caption")
        image_input = gr.Image(type="filepath", label="Upload Image")
        
        output = gr.JSON()
        
        def insert_data(kb_id_val, text, img_path):
            if not kb_id_val:
                return {"error": "KB ID is required"}
            if not img_path:
                return {"error": "Image is required"}
            
            with open(img_path, 'rb') as f:
                files = {'image': f}
                data = {
                    'kb_id': kb_id_val,
                    'text': text or ""
                }
                response = requests.post(
                    f"{BACKEND_URL}/kb/insert",
                    data=data,
                    files=files
                )
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": response.json().get("message", "Insert failed")}

        btn = gr.Button("Insert Data")
        btn.click(insert_data, inputs=[kb_id, text_input, image_input], outputs=output)
    
    return gr.Tab

# 3. 检索数据 Tab
def retrieval_tab():
    with gr.Tab("Retrieve from Knowledge Base"):
        kb_id = gr.Textbox(label="Knowledge Base ID", placeholder="Enter KB ID")
        text_query = gr.Textbox(label="Text Query (optional)", placeholder="Search by text")
        image_query = gr.Image(type="filepath", label="Image Query (optional)")
        top_k = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Top-K Results")
        score_threshold = gr.Slider(minimum=0.0, maximum=1.0, value=0.2, label="Similarity Score Threshold")
        
        output_json = gr.JSON()
        output_gallery = gr.HTML(label="Retrieved Images")

        def retrieve_data(kb_id_val, text, img_path, k, score):
            if not kb_id_val:
                return {"error": "KB ID is required"}, "<div style='color: red;'>KB ID is required</div>"
            if not text and not img_path:
                return {"error": "Either text or image is required for query."}, "<div style='color: red;'>Either text or image is required for query.</div>"

            data = {
                'kb_id': kb_id_val,
                'top_k': int(k),
                'score': float(score)
            }
            if text:
                data['text'] = text

            files = {}
            if img_path:
                files['image'] = open(img_path, 'rb')

            try:
                response = requests.post(
                    f"{BACKEND_URL}/rag/retrieval",
                    data=data,
                    files=files if files else None
                )
            finally:
                if files:
                    files['image'].close()

            if response.status_code == 200:
                result = response.json().get("data", [])
                
                # 创建带分数的图片展示HTML
                gallery_html = "<div style='display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 20px; padding: 10px;'>"
                for item in result:
                    url = item.get("url", "")
                    score_val = item.get("score", 0)
                    text_desc = item.get("text", "")
                    
                    gallery_html += f"""
                    <div style='border: 1px solid #ddd; border-radius: 8px; padding: 10px; text-align: center;'>
                        <img src='{url}' style='max-width: 100%; height: 150px; object-fit: contain; border-radius: 4px;' />
                        <div style='margin-top: 8px; font-size: 14px;'>
                            <div><strong>Score:</strong> {score_val:.4f}</div>
                            <div style='color: #666; font-size: 12px; margin-top: 4px;'>{text_desc}</div>
                        </div>
                    </div>
                    """
                gallery_html += "</div>"
                
                return result, gallery_html
            else:
                error_msg = response.json().get("message", "Retrieval failed")
                return {"error": error_msg}, f"<div style='color: red;'>Error: {error_msg}</div>"

        btn = gr.Button("Retrieve")
        btn.click(
            retrieve_data,
            inputs=[kb_id, text_query, image_query, top_k, score_threshold],
            outputs=[output_json, output_gallery]
        )
    
    return gr.Tab

# 构建主界面
with gr.Blocks(title="Multimodal Knowledge Base Manager") as demo:
    gr.Markdown("# 🌐 多模态知识库管理系统")
    gr.Markdown("使用 Gradio 管理你的图文知识库：创建、插入、检索")

    list_kb_tab()
    create_kb_tab()
    insert_data_tab()
    retrieval_tab()

# 启动应用
if __name__ == "__main__":
    demo.launch()