import pandas as pd # Đảm bảo đã import pandas

# ... (các đoạn code trước đó) ...

# --- BẮT ĐẦU ĐOẠN CODE FIX LỖI ---
target_col = "Link dữ liệu lấy dữ liệu"

# Kiểm tra xem cột có tồn tại trong DataFrame không để tránh lỗi
if target_col in st.session_state['df_config'].columns:
    # Định nghĩa hàm làm sạch dữ liệu
    def clean_cell_data(val):
        if isinstance(val, list):
            # Nếu là list, nối các phần tử lại bằng dấu phẩy
            return ", ".join(map(str, val))
        if pd.isna(val):
            # Nếu là None hoặc NaN, trả về chuỗi rỗng
            return ""
        # Các trường hợp khác ép cứng về kiểu chuỗi
        return str(val)

    # Áp dụng vào cột trong session_state
    st.session_state['df_config'][target_col] = st.session_state['df_config'][target_col].apply(clean_cell_data)
# --- KẾT THÚC ĐOẠN CODE FIX LỖI ---

# Sau đó mới gọi data_editor như cũ
edited_df = st.data_editor(
    st.session_state['df_config'],
    column_config={
        "Link dữ liệu lấy dữ liệu": st.column_config.TextColumn("Link dữ liệu"),
        # ... giữ nguyên các config khác của bạn ...
    },
    key="editor"
)
