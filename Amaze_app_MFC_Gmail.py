import streamlit as st
import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from datetime import datetime, timedelta
from PIL import Image
from pyzbar.pyzbar import decode 
import io 
import time
from googleapiclient.errors import HttpError
import json

# --- DEBUG CONNECTION ---
# st.write("Testing Connection...")
# try:
#     creds = get_credentials()
#     gc = gspread.authorize(creds)
#     sh = gc.open_by_key(SHEET_ID)
#     ws = sh.worksheet(USER_SHEET_NAME)
#     st.success(f"✅ เชื่อมต่อ Google Sheet สำเร็จ! เจอข้อมูล {len(ws.get_all_values())} แถว")
# except Exception as e:
#     st.error(f"❌ เชื่อมต่อไม่ได้: {e}")
#     st.stop()

# --- IMPORT LIBRARY กล้อง ---
try:
    from streamlit_back_camera_input import back_camera_input
except ImportError:
    st.error("⚠️ ต้องเพิ่ม 'streamlit-back-camera-input' ใน requirements.txt")
    st.stop()

# --- CSS HACK ---
st.markdown(
    """
    <style>
    iframe[title="streamlit_back_camera_input.back_camera_input"] {
        min-height: 450px !important; 
        height: 150% !important;
    }
    div[data-testid="stDataFrame"] { width: 100%; }
    </style>
    """,
    unsafe_allow_html=True
)

# --- CONFIGURATION ---
MAIN_FOLDER_ID = '1VjyciJOBhBNCwo9z2iF1WVWXQjTyRkJ2'
SHEET_ID = '1rWgqfrut0H0wRSTocEq04mGGgnZs0T45uaMYZmXVdj8'
LOG_SHEET_NAME = 'Logs'
RIDER_SHEET_NAME = 'Rider_Logs'
USER_SHEET_NAME = 'User'

# --- AUTHENTICATION ---
def get_credentials():
    try:
        if "oauth" in st.secrets:
            info = st.secrets["oauth"]
            creds = Credentials(
                None,
                refresh_token=info["refresh_token"],
                token_uri="https://oauth2.googleapis.com/token",
                client_id=info["client_id"],
                client_secret=info["client_secret"],
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            return creds
        else:
            st.error("❌ ไม่พบข้อมูล [oauth] ใน Secrets")
            return None
    except Exception as e:
        st.error(f"❌ Error Credentials: {e}")
        return None

def authenticate_drive():
    try:
        creds = get_credentials()
        if creds: return build('drive', 'v3', credentials=creds)
        return None
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return None

# --- GOOGLE SERVICES ---
@st.cache_data(ttl=600)
def load_sheet_data(sheet_name=0): 
    try:
        creds = get_credentials()
        if not creds: return pd.DataFrame()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        if isinstance(sheet_name, int): worksheet = sh.get_worksheet(sheet_name)
        else: worksheet = sh.worksheet(sheet_name)
        rows = worksheet.get_all_values()
        if len(rows) > 1:
            headers = rows[0]; data = rows[1:]
            df = pd.DataFrame(data, columns=headers)
            df.columns = df.columns.str.strip()
            for col in df.columns:
                if 'barcode' in col.lower() or 'id' in col.lower(): 
                    df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True)
            if 'Barcode' not in df.columns:
                for col in df.columns:
                    if col.lower() == 'barcode':
                        df.rename(columns={col: 'Barcode'}, inplace=True); break
            return df
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

# --- TIME HELPER ---
def get_thai_time(): return (datetime.utcnow() + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S")
def get_thai_date_str(): return (datetime.utcnow() + timedelta(hours=7)).strftime("%d-%m-%Y")
def get_thai_time_suffix(): return (datetime.utcnow() + timedelta(hours=7)).strftime("%H-%M")
def get_thai_ts_filename(): return (datetime.utcnow() + timedelta(hours=7)).strftime("%Y%m%d_%H%M%S")

def save_log_to_sheet(picker_name, order_id, barcode, prod_name, location, pick_qty, user_col, file_id):
    try:
        creds = get_credentials(); gc = gspread.authorize(creds); sh = gc.open_by_key(SHEET_ID)
        try: worksheet = sh.worksheet(LOG_SHEET_NAME)
        except: worksheet = sh.add_worksheet(title=LOG_SHEET_NAME, rows="1000", cols="20"); worksheet.append_row(["Timestamp", "Picker Name", "Order ID", "Barcode", "Product Name", "Location", "Pick Qty", "User", "Image Link (Col I)"])
        timestamp = get_thai_time(); image_link = f"https://drive.google.com/open?id={file_id}"
        worksheet.append_row([timestamp, picker_name, order_id, barcode, prod_name, location, pick_qty, user_col, image_link])
    except Exception as e: st.warning(f"⚠️ บันทึก Log ไม่สำเร็จ: {e}")

def save_rider_log(picker_name, order_id, file_id, folder_name):
    try:
        creds = get_credentials(); gc = gspread.authorize(creds); sh = gc.open_by_key(SHEET_ID)
        try: worksheet = sh.worksheet(RIDER_SHEET_NAME)
        except: worksheet = sh.add_worksheet(title=RIDER_SHEET_NAME, rows="1000", cols="10"); worksheet.append_row(["Timestamp", "User Name", "Order ID", "Folder Name", "Rider Image Link"])
        timestamp = get_thai_time(); image_link = f"https://drive.google.com/open?id={file_id}"
        worksheet.append_row([timestamp, picker_name, order_id, folder_name, image_link])
    except Exception as e: st.warning(f"⚠️ บันทึก Rider Log ไม่สำเร็จ: {e}")

# --- [MODIFIED] FOLDER STRUCTURE LOGIC ---
def get_target_folder_structure(service, order_id, main_parent_id):
    # คำนวณวันเวลาปัจจุบัน
    now = datetime.utcnow() + timedelta(hours=7)
    year_str = now.strftime("%Y")
    month_str = now.strftime("%m")
    date_str = now.strftime("%d-%m-%Y")

    # Helper function: ค้นหาหรือสร้าง Folder
    def _get_or_create(parent_id, name):
        q = f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        res = service.files().list(q=q, fields="files(id)").execute()
        files = res.get('files', [])
        if files: return files[0]['id']
        
        # ถ้าไม่เจอให้สร้างใหม่
        meta = {'name': name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
        folder = service.files().create(body=meta, fields='id').execute()
        return folder.get('id')

    # Step 1: จัดการ Folder ปี (YYYY)
    year_id = _get_or_create(main_parent_id, year_str)
    
    # Step 2: จัดการ Folder เดือน (MM)
    month_id = _get_or_create(year_id, month_str)
    
    # Step 3: จัดการ Folder วันที่ (DD-MM-YYYY)
    date_id = _get_or_create(month_id, date_str)

    # Step 4: สร้าง Folder Order (OrderNumber_HH-MM)
    time_suffix = now.strftime("%H-%M")
    order_folder_name = f"{order_id}_{time_suffix}"
    meta_order = {'name': order_folder_name, 'parents': [date_id], 'mimeType': 'application/vnd.google-apps.folder'}
    order_folder = service.files().create(body=meta_order, fields='id').execute()
    
    return order_folder.get('id')

def find_existing_order_folder(service, order_id, main_parent_id):
    # คำนวณวันเวลาปัจจุบันเพื่อหา Path
    now = datetime.utcnow() + timedelta(hours=7)
    year_str = now.strftime("%Y")
    month_str = now.strftime("%m")
    date_str = now.strftime("%d-%m-%Y")

    # Helper function: ค้นหา Folder (คืนค่า None ถ้าไม่เจอ)
    def _find_folder(parent_id, name):
        q = f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        res = service.files().list(q=q, fields="files(id)").execute()
        files = res.get('files', [])
        return files[0]['id'] if files else None

    # Step 1: หา Folder ปี (YYYY)
    year_id = _find_folder(main_parent_id, year_str)
    if not year_id: return None, "ไม่พบ Folder ปีปัจจุบัน"

    # Step 2: หา Folder เดือน (MM)
    month_id = _find_folder(year_id, month_str)
    if not month_id: return None, "ไม่พบ Folder เดือนปัจจุบัน"

    # Step 3: หา Folder วันที่ (DD-MM-YYYY)
    date_id = _find_folder(month_id, date_str)
    if not date_id: return None, "ไม่พบ Folder วันที่ของวันนี้ (ยังไม่มีการเปิดบิลวันนี้)"
    
    # Step 4: หา Folder Order ภายใต้ Folder วันที่
    # 1. ค้นหาแบบกว้างๆ ก่อน
    q_order = f"'{date_id}' in parents and name contains '{order_id}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    res_order = service.files().list(q=q_order, fields="files(id, name)", orderBy="createdTime desc").execute()
    files_order = res_order.get('files', [])
    
    # 2. กรองให้ชัวร์ว่าขึ้นต้นด้วย OrderID_
    target_prefix = f"{order_id}_" # เช่น "B01_"
    
    found_folder = None
    for f in files_order:
        if f['name'].startswith(target_prefix):
            found_folder = f
            break
            
    if found_folder:
        return found_folder['id'], found_folder['name']
    else:
        return None, f"ไม่พบ Folder ของ Order: {order_id} ในวันนี้"
# ---------------------------------------------

def upload_photo(service, file_obj, filename, folder_id):
    try:
        file_metadata = {'name': filename, 'parents': [folder_id]}
        
        if isinstance(file_obj, bytes): 
            media_body = io.BytesIO(file_obj)
        else: 
            media_body = file_obj 
            
        media = MediaIoBaseUpload(media_body, mimetype='image/jpeg', chunksize=1024*1024, resumable=True)
        
        # --- จุดที่แก้: เพิ่มการดักจับ Error เพื่อดูรายละเอียด ---
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    except HttpError as error:
        # แปลง Error เป็นข้อความที่อ่านออก
        error_reason = json.loads(error.content.decode('utf-8'))
        print(f"❌ DRIVE ERROR DETAILS: {error_reason}") # จะโชว์ใน Logs ของ Streamlit Cloud
        st.error(f"Google Drive Error: {error_reason}") # จะโชว์หน้าจอ App
        raise error # ส่ง Error ต่อไปให้โปรแกรมหยุดทำงาน
    except Exception as e:
        print(f"❌ GENERAL ERROR: {e}")
        raise e

# --- SAFE RESET SYSTEM ---
def trigger_reset():
    st.session_state.need_reset = True

def check_and_execute_reset():
    if st.session_state.get('need_reset'):
        # Reset Widgets
        if 'pack_order_man' in st.session_state: st.session_state.pack_order_man = ""
        if 'rider_ord_man' in st.session_state: st.session_state.rider_ord_man = ""
        if 'pack_prod_man' in st.session_state: st.session_state.pack_prod_man = ""
        if 'loc_man' in st.session_state: st.session_state.loc_man = ""
        
        # Reset State Variables
        st.session_state.order_val = ""
        st.session_state.current_order_items = []
        st.session_state.photo_gallery = [] 
        st.session_state.rider_photo = None
        st.session_state.picking_phase = 'scan'
        st.session_state.temp_login_user = None
        
        # --- NEW: Clear Target Folder State to avoid stale data ---
        st.session_state.target_rider_folder_id = None
        st.session_state.target_rider_folder_name = ""
        
        # Reset Helpers
        st.session_state.prod_val = ""
        st.session_state.loc_val = ""
        st.session_state.prod_display_name = ""
        st.session_state.pick_qty = 1 
        st.session_state.cam_counter += 1
        
        st.session_state.need_reset = False

def logout_user():
    st.session_state.current_user_name = ""
    st.session_state.current_user_id = ""
    trigger_reset()
    st.rerun()

# --- UI SETUP ---
st.set_page_config(page_title="Smart Picking System", page_icon="📦")

def init_session_state():
    if 'need_reset' not in st.session_state: st.session_state.need_reset = False
    keys = ['current_user_name', 'current_user_id', 'order_val', 'prod_val', 'loc_val', 'prod_display_name', 
            'photo_gallery', 'cam_counter', 'pick_qty', 'rider_photo', 'current_order_items', 'picking_phase', 'temp_login_user',
            'target_rider_folder_id', 'target_rider_folder_name'] # Added target folder vars
    for k in keys:
        if k not in st.session_state:
            if k == 'pick_qty': st.session_state[k] = 1
            elif k == 'cam_counter': st.session_state[k] = 0
            elif k == 'photo_gallery': st.session_state[k] = []
            elif k == 'current_order_items': st.session_state[k] = []
            elif k == 'picking_phase': st.session_state[k] = 'scan'
            else: st.session_state[k] = None if k in ['temp_login_user', 'target_rider_folder_id'] else ""

init_session_state()
check_and_execute_reset()

# --- LOGIN ---
if not st.session_state.current_user_name:
    st.title("🔐 Login พนักงาน")
    df_users = load_sheet_data(USER_SHEET_NAME)

    if st.session_state.temp_login_user is None:
        st.info("กรุณาสแกนรหัสพนักงาน")
        col1, col2 = st.columns([3, 1])
        manual_user = col1.text_input("พิมพ์รหัสพนักงาน", key="input_user_manual").strip()
        cam_key_user = f"cam_user_{st.session_state.cam_counter}"
        scan_user = back_camera_input("แตะเพื่อสแกนบัตรพนักงาน", key=cam_key_user)
        
        user_input_val = None
        if manual_user: user_input_val = manual_user
        elif scan_user:
            res_u = decode(Image.open(scan_user))
            if res_u: user_input_val = res_u[0].data.decode("utf-8")
        
        if user_input_val:
            if not df_users.empty and len(df_users.columns) >= 3:
                match = df_users[df_users.iloc[:, 0].astype(str) == str(user_input_val)]
                if not match.empty:
                    st.session_state.temp_login_user = {'id': str(user_input_val), 'pass': str(match.iloc[0, 1]).strip(), 'name': match.iloc[0, 2]}
                    st.rerun()
                else: st.error(f"❌ ไม่พบรหัสพนักงาน: {user_input_val}")
            else: st.warning("⚠️ โหลดข้อมูลพนักงานไม่ได้")
    else:
        user_info = st.session_state.temp_login_user
        st.info(f"👤 พนักงาน: **{user_info['name']}** ({user_info['id']})")
        password_input = st.text_input("🔑 กรุณากรอกรหัสผ่าน", type="password", key="login_pass_input").strip()
        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("✅ ยืนยัน Login", type="primary", use_container_width=True):
                if password_input == user_info['pass']:
                    st.session_state.current_user_id = user_info['id']
                    st.session_state.current_user_name = user_info['name']
                    st.session_state.temp_login_user = None
                    st.toast(f"ยินดีต้อนรับคุณ {user_info['name']} 👋", icon="✅")
                    time.sleep(1); st.rerun()
                else: st.error("❌ รหัสผ่านไม่ถูกต้อง")
        with c2:
            if st.button("⬅️ เปลี่ยน User", use_container_width=True):
                st.session_state.temp_login_user = None; st.rerun()
else:
    # --- LOGGED IN ---
    with st.sidebar:
        st.write(f"👤 **{st.session_state.current_user_name}**")
        mode = st.radio("เลือกโหมดทำงาน:", ["📦 แผนกแพ็คสินค้า", "🏍️ ส่งงาน Rider"])
        st.divider()
        if st.button("Logout", type="secondary"): logout_user()

    # ================= MODE 1: PACKING =================
    if mode == "📦 แผนกแพ็คสินค้า":
        st.title("📦 ระบบเบิก-แพ็คสินค้า")
        df_items = load_sheet_data(0)

        if st.session_state.picking_phase == 'scan':
            st.markdown("#### 1. Order ID")
            if not st.session_state.order_val:
                col1, col2 = st.columns([3, 1])
                manual_order = col1.text_input("พิมพ์ Order ID", key="pack_order_man").strip().upper()
                if manual_order: st.session_state.order_val = manual_order; st.rerun()
                scan_order = back_camera_input("แตะเพื่อสแกน Order", key=f"pack_cam_{st.session_state.cam_counter}")
                if scan_order:
                    res = decode(Image.open(scan_order))
                    if res: st.session_state.order_val = res[0].data.decode("utf-8").upper(); st.rerun()
            else:
                c1, c2 = st.columns([3, 1])
                with c1: st.success(f"📦 Order: **{st.session_state.order_val}**")
                with c2: 
                    if st.button("เปลี่ยน Order"): trigger_reset(); st.rerun()

            if st.session_state.order_val:
                st.markdown("---"); st.markdown("#### 2. เพิ่มรายการสินค้า (Scan & Add)")
                if not st.session_state.prod_val:
                    col1, col2 = st.columns([3, 1])
                    manual_prod = col1.text_input("พิมพ์ Barcode", key="pack_prod_man").strip()
                    if manual_prod: st.session_state.prod_val = manual_prod; st.rerun()
                    scan_prod = back_camera_input("แตะเพื่อสแกนสินค้า", key=f"prod_cam_{st.session_state.cam_counter}")
                    if scan_prod:
                        res_p = decode(Image.open(scan_prod))
                        if res_p: st.session_state.prod_val = res_p[0].data.decode("utf-8"); st.rerun()
                else:
                    target_loc_str = None; prod_found = False
                    if not df_items.empty:
                        match = df_items[df_items['Barcode'] == st.session_state.prod_val]
                        if not match.empty:
                            prod_found = True; row = match.iloc[0]
                            try: brand = str(row.iloc[3]); variant = str(row.iloc[5]); full_name = f"{brand} {variant}"
                            except: full_name = "Error Name"
                            st.session_state.prod_display_name = full_name
                            target_loc_str = f"{str(row.get('Zone','')).strip()}-{str(row.get('Location','')).strip()}"
                            st.success(f"✅ **{full_name}**"); st.warning(f"📍 เป้าหมาย: **{target_loc_str}**")
                        else: st.error("❌ ไม่พบ Barcode")
                    else: st.warning("⚠️ Loading Data...")
                    
                    if st.button("❌ สแกนใหม่"): 
                        st.session_state.prod_val = ""; st.session_state.cam_counter += 1; st.rerun()

                    if prod_found and target_loc_str:
                        st.markdown("---"); st.markdown("##### ยืนยัน Location")
                        if not st.session_state.loc_val:
                            man_loc = st.text_input("Scan/พิมพ์ Location", key="loc_man").strip().upper()
                            if man_loc: st.session_state.loc_val = man_loc; st.rerun()
                            scan_loc = back_camera_input("แตะเพื่อสแกน Location", key=f"loc_cam_{st.session_state.cam_counter}")
                            if scan_loc:
                                res_l = decode(Image.open(scan_loc))
                                if res_l: st.session_state.loc_val = res_l[0].data.decode("utf-8").upper(); st.rerun()
                        else:
                            if st.session_state.loc_val == target_loc_str or st.session_state.loc_val in target_loc_str:
                                st.success(f"✅ ถูกต้อง: {st.session_state.loc_val}")
                                st.markdown("##### ระบุจำนวน")
                                st.session_state.pick_qty = st.number_input("จำนวน (Qty)", min_value=1, value=1)
                                st.markdown("---")
                                if st.button("➕ เพิ่มลงตะกร้า", type="primary", use_container_width=True):
                                    new_item = {"Barcode": st.session_state.prod_val, "Product Name": st.session_state.prod_display_name, "Location": st.session_state.loc_val, "Qty": st.session_state.pick_qty}
                                    st.session_state.current_order_items.append(new_item)
                                    st.toast(f"เพิ่ม {st.session_state.prod_display_name} แล้ว!", icon="🛒")
                                    st.session_state.prod_val = ""; st.session_state.loc_val = ""; st.session_state.pick_qty = 1; st.session_state.cam_counter += 1
                                    st.rerun()
                            else:
                                st.error(f"❌ ผิดตำแหน่ง ({st.session_state.loc_val})")
                                if st.button("แก้ Location"): st.session_state.loc_val = ""; st.rerun()

                if st.session_state.current_order_items:
                    st.markdown("---")
                    st.markdown(f"### 🛒 ตะกร้าสินค้า ({len(st.session_state.current_order_items)} รายการ)")
                    st.dataframe(pd.DataFrame(st.session_state.current_order_items), use_container_width=True)
                    if st.button("✅ ยืนยันรายการครบแล้ว (ไปถ่ายรูป)", type="primary", use_container_width=True):
                        st.session_state.picking_phase = 'pack'; st.rerun()

        elif st.session_state.picking_phase == 'pack':
            st.success(f"📦 Order: **{st.session_state.order_val}** (ยืนยันแล้ว)")
            st.info("รายการสินค้าที่จะแพ็ค:")
            st.dataframe(pd.DataFrame(st.session_state.current_order_items), use_container_width=True)
            st.markdown("#### 3. ถ่ายรูปปิดกล่อง (รวมทุกชิ้น)")
            
            if st.session_state.photo_gallery:
                cols = st.columns(5)
                for idx, img in enumerate(st.session_state.photo_gallery):
                    with cols[idx]:
                        st.image(img, use_column_width=True)
                        if st.button("🗑️", key=f"del_{idx}"): st.session_state.photo_gallery.pop(idx); st.rerun()
            
            if len(st.session_state.photo_gallery) < 5:
                pack_img = back_camera_input("ถ่ายรูปสินค้ากองรวม (กล้องหลัง)", key=f"pack_cam_fin_{st.session_state.cam_counter}")
                if pack_img:
                    img_pil = Image.open(pack_img)
                    if img_pil.mode in ("RGBA", "P"): img_pil = img_pil.convert("RGB")
                    buf = io.BytesIO(); img_pil.save(buf, format='JPEG')
                    st.session_state.photo_gallery.append(buf.getvalue())
                    st.session_state.cam_counter += 1; st.rerun()
            
            col_b1, col_b2 = st.columns([1, 1])
            with col_b1:
                if st.button("⬅️ กลับไปแก้ไขรายการ"): st.session_state.picking_phase = 'scan'; st.session_state.photo_gallery = []; st.rerun()
            with col_b2:
                if len(st.session_state.photo_gallery) > 0:
                    if st.button("☁️ ยืนยัน Upload ทั้งหมด", type="primary", use_container_width=True):
                        with st.spinner("กำลังบันทึกข้อมูล..."):
                            srv = authenticate_drive()
                            if srv:
                                fid = get_target_folder_structure(srv, st.session_state.order_val, MAIN_FOLDER_ID)
                                ts = get_thai_ts_filename()
                                
                                # 1. หาจำนวนรูปทั้งหมดก่อน
                                total_imgs = len(st.session_state.photo_gallery)
                                final_image_link_id = "" # เตรียมตัวแปรไว้เก็บ ID รูปสุดท้าย

                                for i, b in enumerate(st.session_state.photo_gallery):
                                    # i เริ่มที่ 0, 1, 2...
                                    current_seq = i + 1 
                                    
                                    # ตั้งชื่อไฟล์ให้มีลำดับชัดเจน Img1, Img2, ...
                                    fn = f"{st.session_state.order_val}_PACKED_{ts}_Img{current_seq}.jpg"
                                    uid = upload_photo(srv, b, fn, fid)
                                    
                                    # 2. เช็คเงื่อนไข: "ถ้านี่คือรอบสุดท้าย ให้จำ ID นี้ไว้"
                                    if current_seq == total_imgs:
                                        final_image_link_id = uid
                                
                                # 3. บันทึกลง Sheet (ใช้ ID ที่เราดักจับไว้)
                                # ถ้าไม่มีรูปเลย (กัน Error) ให้ใส่ขีด -
                                if not final_image_link_id: final_image_link_id = "-"

                                for item in st.session_state.current_order_items:
                                    save_log_to_sheet(
                                        st.session_state.current_user_name, 
                                        st.session_state.order_val, 
                                        item['Barcode'], 
                                        item['Product Name'], 
                                        item['Location'], 
                                        item['Qty'], 
                                        st.session_state.current_user_id, 
                                        final_image_link_id  # <--- ส่ง Link รูปสุดท้ายไปบันทึก
                                    )
                                    
                                st.balloons()
                                st.success("✅ บันทึกครบทุกรายการเรียบร้อย!")
                                time.sleep(1.5)
                                trigger_reset()
                                st.rerun()

    # ================= MODE 2: RIDER =================
    elif mode == "🏍️ ส่งงาน Rider":
        st.title("🏍️ ส่งงาน Rider")
        st.info("ถ่ายรูปเพิ่มเติมเพื่อส่งให้ Rider (จะบันทึกลง Folder เดิม)")

        st.markdown("#### 1. สแกน Order ที่จะส่ง")
        col_r1, col_r2 = st.columns([3, 1])
        man_rider_ord = col_r1.text_input("พิมพ์ Order ID", key="rider_ord_man").strip().upper()
        
        # Camera Input
        scan_rider_ord = back_camera_input("แตะเพื่อสแกน Order", key=f"rider_cam_ord_{st.session_state.cam_counter}")
        
        current_rider_order = ""
        if man_rider_ord: current_rider_order = man_rider_ord
        elif scan_rider_ord:
            res = decode(Image.open(scan_rider_ord))
            if res: current_rider_order = res[0].data.decode("utf-8").upper()

        if current_rider_order:
            st.session_state.order_val = current_rider_order
            with st.spinner(f"🔍 กำลังหา Folder ของ {current_rider_order}..."):
                srv = authenticate_drive()
                if srv:
                    folder_id, folder_name = find_existing_order_folder(srv, current_rider_order, MAIN_FOLDER_ID)
                    if folder_id:
                        st.success(f"✅ เจอ Folder: **{folder_name}**")
                        st.session_state.target_rider_folder_id = folder_id; st.session_state.target_rider_folder_name = folder_name
                    else: 
                        st.error(f"❌ {folder_name}")
                        st.session_state.target_rider_folder_id = None
                        st.session_state.target_rider_folder_name = ""

        if st.session_state.get('target_rider_folder_id') and st.session_state.order_val:
            st.markdown("---"); st.markdown(f"#### 2. ถ่ายรูปส่งมอบ ({st.session_state.target_rider_folder_name})")
            rider_img_input = back_camera_input("ถ่ายรูปส่งมอบ", key=f"rider_cam_act_{st.session_state.cam_counter}")
            
            if rider_img_input:
                st.image(rider_img_input, caption="รูปที่จะส่ง", width=300)
                col_upload, col_clear = st.columns([2, 1])
                with col_clear:
                    if st.button("🗑️ ซ่อน/ถ่ายใหม่", type="secondary", use_container_width=True):
                         st.session_state.cam_counter += 1; st.rerun()
                with col_upload:
                    if st.button("🚀 ยืนยันส่งรูปนี้", type="primary", use_container_width=True):
                        with st.spinner("Uploading..."):
                            srv = authenticate_drive()
                            ts = get_thai_ts_filename()
                            fn = f"RIDER_{st.session_state.order_val}_{ts}.jpg"
                            uid = upload_photo(srv, rider_img_input, fn, st.session_state.target_rider_folder_id)
                            save_rider_log(st.session_state.current_user_name, st.session_state.order_val, uid, st.session_state.target_rider_folder_name)
                            st.success("บันทึกรูป Rider สำเร็จ!")
                            time.sleep(1.5)
                            trigger_reset(); st.rerun()
