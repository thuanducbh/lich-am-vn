# -*- coding: utf-8 -*-
# Script để nhập lịch âm từ file ICS cục bộ vào Google Calendar
# Có bổ sung chức năng kiểm tra trùng lặp (chỉ dựa trên ngày) và thêm tên ngày lễ vào tiêu đề
# Sử dụng batching để tối ưu tốc độ và tuân thủ rate limit API

# Import các thư viện cần thiết
from datetime import date, datetime, timedelta
import os
import time
import re # Để xử lý biểu thức chính quy
import json
import sys # Cần để in lỗi ra stderr
# import traceback # Có thể uncomment nếu muốn debug sâu hơn các lỗi

# Import các thư thư viện Google API
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.http import BatchHttpRequest # Cần BatchHttpRequest để sử dụng Batching

# Import thư viện xử lý file ICS
from icalendar import Calendar

# Import thư viện xử lý múi giờ
from pytz import timezone # Cần pip install pytz
import pytz # Đảm bảo đã import pytz

# --- HẰNG SỐ CẤU HÌNH ---
SCOPES = ['https://www.googleapis.com/auth/calendar'] # Scope cần thiết để truy cập và quản lý Google Calendar

# --- Cấu hình nguồn file ICS (file cục bộ thay vì URL) ---
# Tên file ICS đầu vào của fen trong cùng thư mục với script
LOCAL_ICS_INPUT_FILE = 'amlichvn.ics' # <-- ĐẶT TÊN FILE ICS CỦA FEN Ở ĐÂY
# Tên file sẽ lưu nội dung ICS sau khi sửa định dạng (file tạm cục bộ)
# Script sẽ đọc file LOCAL_ICS_INPUT_FILE, sửa lỗi, và lưu vào ICS_FILE để các hàm nhập đọc.
ICS_FILE = 'amlichvn_local.ics'

# Cấu hình lịch Google Calendar đích
CALENDAR_NAME = 'Lịch ÂM VN' # Tên lịch trên Google Calendar mà bạn muốn nhập sự kiện vào
VIETNAM_TZ = timezone('Asia/Ho_Chi_Minh') # Múi giờ cho lịch (chọn múi giờ Việt Nam)

# --- CẤU HÌNH BATCHING ---
# Số lượng sự kiện (yêu cầu API) trong mỗi gói batch khi gửi lên Google Calendar API.
# Google khuyến nghị không quá 1000 yêu cầu/batch. 100 là con số tốt cho Batching.
BATCH_SIZE = 100

# Cấu hình kiểm soát tốc độ nhập sự kiện (để tuân thủ giới hạn API 600 truy vấn/phút)
# Với batching, chúng ta kiểm soát tốc độ giữa các lần thực thi batch.
# Tổng số yêu cầu/phút <= 600.
# Số batch/phút * Kích thước batch <= 600.
# Thời gian tối thiểu giữa các lần thực thi batch ≈ Kích thước batch / (600 yêu cầu/phút / 60 giây/phút)
# ≈ Kích thước batch / 10 yêu cầu/giây
# Đặt một khoảng đệm an toàn. Ví dụ: batch 100 requests, cần ít nhất 100 / 9 ≈ 11.11 giây giữa các batch.
MIN_DELAY_BETWEEN_BATCHES = BATCH_SIZE / 9.0 # Khoảng 11.11 giây cho batch 100

# --- HẰNG SỐ CẤU HÌNH BỔ SUNG CHO CHỨC NĂNG KIỂM TRA TRÙNG LẶP ---
# Phạm vi năm để kiểm tra sự kiện đã tồn tại trên Google Calendar
# Nên khớp với phạm vi năm của file ICS được tạo ra (ví dụ: 1900-2100)
START_YEAR_ICS = 1900
END_YEAR_ICS = 2100


# --- DANH SÁCH CÁC NGÀY LỄ ĐẶC BIỆT (LỊCH ÂM) ---
# Dictionary ánh xạ "ngày/tháng âm lịch" sang tên ngày lễ.
# Sử dụng để xác định ngày lễ, gán màu sắc VÀ BỔ SUNG TÊN LỄ VÀO TIÊU ĐỀ.
special_days = {
    "1/1": "Tết Nguyên đán",
    "15/1": "Tết Nguyên Tiêu",
    "3/3": "Tết Hàn thực",
    "10/3": "Giỗ tổ Hùng Vương", # Theo lịch âm
    "15/4": "Lễ Phật Đản",
    "5/5": "Tết Đoan ngọ",
    "7/7": "Lễ Thất tịch",
    "15/7": "Lễ Vu Lan",
    "15/8": "Tết Trung thu",
    "9/9": "Tết Trùng cửu",
    "10/10": "Tết Trùng thập",
    "15/10": "Tết Hạ Nguyên",
    "23/12": "Ông Táo về trời" # 23 tháng Chạp âm lịch
}
# Bạn có thể chỉnh sửa dictionary này nếu cần thêm/bớt ngày lễ hoặc sửa tên.


# --- HÀM XÁC THỰC VÀ LẤY GOOGLE SERVICE ---
def get_service():
    """
    Xác thực với Google API sử dụng OAuth 2.0.
    Ưu tiên dùng token đã lưu (token.json), nếu hết hạn thì làm mới.
    Nếu chưa có hoặc làm mới lỗi, thực hiện luồng xác thực mới từ credentials.json.
    Trả về đối tượng service cho Google Calendar API hoặc None nếu lỗi.
    """
    creds = None
    # Bước 1: Thử tải credential từ file token.json đã lưu
    if os.path.exists('token.json'):
        try:
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        except Exception as e:
            print(f"Cảnh báo: Lỗi đọc token.json - {e}. Sẽ tiến hành xác thực mới.", file=sys.stderr)
            creds = None # Reset creds nếu đọc lỗi
    # Bước 2: Nếu credential đã tải về không hợp lệ hoặc đã hết hạn, thử làm mới
    if creds and not creds.valid:
        if creds.expired and creds.refresh_token:
            print("Token hết hạn, đang làm mới token xác thực...")
            try:
                # Thực hiện làm mới token sử dụng refresh_token
                creds.refresh(Request())
                print("Đã làm mới token thành công.")
            except Exception as e:
                print(f"Lỗi làm mới token: {e}. Cần xác thực lại từ đầu.", file=sys.stderr)
                # Lỗi 'invalid_scope' khi refresh cũng rơi vào đây.
                if "invalid_scope" in str(e):
                     print("Lỗi: Phạm vi xác thực (scope) không hợp lệ. Vui lòng kiểm tra lại SCOPES trong code và cấu hình dự án Google Cloud.", file=sys.stderr)
                creds = None # Reset creds nếu làm mới lỗi
        else:
             # Token không có refresh_token hoặc lỗi khác khiến token không hợp lệ
             print("Token không hợp lệ hoặc không thể làm mới, cần xác thực lại từ đầu.", file=sys.stderr)
             creds = None # Reset creds
    # Bước 3: Nếu chưa có credential hợp lệ (lần đầu chạy hoặc xác thực lại), thực hiện luồng xác thực mới
    if not creds or not creds.valid:
        if os.path.exists('credentials.json'):
            print("Đang tiến hành xác thực mới từ credentials.json...")
            try:
                # Khởi tạo luồng OAuth 2.0 từ file client secrets
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                # Chạy server cục bộ để lắng nghe kết quả xác thực từ trình duyệt
                # Với Termux và port=0 thường hoạt động nếu bạn có trình duyệt trên điện thoại.
                creds = flow.run_local_server(port=0) # Mở cổng ngẫu nhiên và chờ xác thực qua trình duyệt
                print("Xác thực thành công.")
            except FileNotFoundError:
                 print("Lỗi: Không tìm thấy tệp credentials.json!", file=sys.stderr)
                 print("Vui lòng tải file credentials.json từ Google Cloud Console và đặt cùng thư mục với script.", file=sys.stderr)
                 return None
            except Exception as e:
                 print(f"Lỗi trong quá trình xác thực mới: {e}", file=sys.stderr)
                 print("Đảm bảo bạn đã cài google-auth-oauthlib và kết nối mạng ổn định.", file=sys.stderr)
                 # Lỗi 'could not locate runnable browser' cũng rơi vào đây.
                 if "could not locate runnable browser" in str(e):
                     print("Có vẻ như script không tìm thấy trình duyệt để hoàn tất xác thực OAuth.", file=sys.stderr)
                     print("Nếu bạn chạy trên máy chủ không có GUI hoặc môi trường hạn chế (như Termux), bạn cần phải truy cập thủ công URL xác thực.", file=sys.stderr)
                     print("Khi script in ra URL, copy và mở nó trong trình duyệt trên thiết bị khác.", file=sys.stderr)
                     print("Sau khi xác thực trên trình duyệt, nếu thấy lỗi trang web hoặc mã, copy mã đó và dán lại vào terminal nếu script chờ nhập.", file=sys.stderr)
                 return None
        else:
            print("Lỗi: Không tìm thấy tệp credentials.json! Cần file này để xác thực lần đầu nếu token.json không tồn tại hoặc lỗi.", file=sys.stderr)
            return None
    # Bước 4: Lưu credential (có thể đã được làm mới hoặc mới tạo) vào file token.json cho lần chạy sau
    try:
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
        # print("Đã lưu token.json thành công cho lần chạy sau.")
    except Exception as e:
         print(f"Cảnh báo: Không thể lưu token.json - {e}. Token sẽ không được lưu cho lần chạy sau.", file=sys.stderr)
    # Bước 5: Xây dựng và trả về đối tượng service cho Google Calendar API
    try:
        service = build('calendar', 'v3', credentials=creds)
        return service
    except Exception as e:
        print(f"Lỗi khi xây dựng service hoặc kết nối API: {e}", file=sys.stderr)
        print("Đảm bảo bạn đã cài google-api-python-client và kết nối mạng ổn định.", file=sys.stderr)
        return None

# --- HÀM TÌM HOẶC TẠO LỊCH GOOGLE ---
def get_or_create_calendar(service):
    """
    Tìm lịch Google Calendar theo tên đã cấu hình (CALENDAR_NAME).
    Nếu không tìm thấy, tạo một lịch mới với tên đó và múi giờ Việt Nam.
    Trả về ID của lịch hoặc None nếu lỗi.
    """
    if not service:
        print("Không có đối tượng service API, không thể tìm hoặc tạo lịch.", file=sys.stderr)
        return None
    try:
        print(f"Đang tìm lịch Google Calendar có tên '{CALENDAR_NAME}'...")
        page_token = None
        # API list calendarList có thể trả về kết quả theo trang
        while True:
            # Lấy danh sách các lịch của người dùng (lấy từng trang)
            calendar_list = service.calendarList().list(pageToken=page_token).execute()
            calendars = calendar_list.get('items', [])
            # Duyệt qua danh sách lịch trong trang hiện tại để tìm lịch theo tên
            for cal in calendars:
                if cal.get('summary') == CALENDAR_NAME:
                    print(f"Đã tìm thấy lịch: ID = {cal['id']}")
                    return cal['id'] # Trả về ID của lịch nếu tìm thấy
            # Kiểm tra xem còn trang kết quả tiếp theo không
            page_token = calendar_list.get('nextPageToken')
            if not page_token:
                break # Nếu không còn pageToken, tức là đã duyệt hết tất cả các lịch

        # Nếu đã duyệt hết tất cả các lịch mà không tìm thấy lịch theo tên
        print(f"Không tìm thấy lịch '{CALENDAR_NAME}'. Đang tạo một lịch mới...")
        # Định nghĩa thông tin cho lịch mới
        calendar = {
            'summary': CALENDAR_NAME, # Tên lịch mới
            'timeZone': 'Asia/Ho_Chi_Minh' # Đặt múi giờ cho lịch mới
        }
        # Gọi API để tạo lịch mới
        created_calendar = service.calendars().insert(body=calendar).execute()
        print(f"Đã tạo lịch mới thành công: Tên = '{created_calendar['summary']}', ID = {created_calendar['id']}")
        return created_calendar['id'] # Trả về ID của lịch vừa tạo
    except Exception as e:
        print(f"Lỗi khi tìm hoặc tạo lịch Google Calendar: {e}", file=sys.stderr)
        print("Đảm bảo tài khoản Google đã xác thực có quyền xem và quản lý lịch.", file=sys.stderr)
        return None

# --- HÀM SỬA LỖI ĐỊNH DẠNG FILE ICS (ĐÃ CÓ TỪ CODE GỐC VÀ ĐƯỢC TINH CHỈNH) ---
def fix_ics_format(raw_text):
    """
    Cố gắng sửa các lỗi định dạng phổ biến trong nội dung ICS mà thư viện icalendar có thể khó xử lý:
    - Loại bỏ khoảng trắng thừa sau dấu hai chấm (:) ở các dòng thuộc tính cơ bản.
    - Cố gắng sửa định dạng DTSTART/DTEND sai phổ biến (MicheleMMDD0000000Z -> MicheleMMDDTHHMMSSZ).
    Hàm này là một biện pháp tạm thời để làm sạch dữ liệu nguồn.
    """
    lines = raw_text.strip().splitlines()
    final_fixed_lines = []
    # Biểu thức chính quy để bắt các dòng thuộc tính: Tên (tham số tùy chọn) : Giá trị
    prop_line_pattern = re.compile(r'^([A-Z-]+)(;[^:]*)?:(.*)$')
    # Biểu thức chính quy để tìm giá trị của DTSTART/DTEND có vẻ sai (MicheleMMDD theo sau bởi 7 chữ số).
    dt_malformed_val_pattern = re.compile(r'^(\d{8})(\d{7})(Z?)$')

    print("Đang tiến hành sửa lỗi định dạng cơ bản trong nội dung ICS...")
    space_fix_count = 0
    dt_fix_count = 0

    # Pass 1: Sửa lỗi khoảng trắng sau dấu hai chấm
    temp_lines_pass1 = []
    for line in lines:
        stripped_line = line.strip()
        if not stripped_line:
            continue # Bỏ qua dòng trống

        match = prop_line_pattern.match(stripped_line)
        if match:
            prop_name = match.group(1)
            prop_params = match.group(2) or ''
            colon_index_after_params = stripped_line.find(':', len(prop_name) + len(prop_params))

            if colon_index_after_params != -1 and colon_index_after_params + 1 < len(stripped_line):
                 if stripped_line[colon_index_after_params + 1] == ' ':
                     line_after_space_fix = stripped_line[:colon_index_after_params + 1] + stripped_line[colon_index_after_params + 2:]
                     temp_lines_pass1.append(line_after_space_fix)
                     space_fix_count += 1
                 else:
                     temp_lines_pass1.append(stripped_line)
            else:
                 temp_lines_pass1.append(stripped_line) # Dòng khớp mẫu nhưng cấu trúc bất thường, giữ nguyên sau strip
        else:
            temp_lines_pass1.append(stripped_line) # Dòng không khớp mẫu thuộc tính, giữ nguyên sau strip

    # Pass 2: Sửa định dạng DTSTART/DTEND sai
    for line in temp_lines_pass1: # Duyệt qua kết quả của Pass 1
         if line.startswith('DTSTART:') or line.startswith('DTEND:'):
             parts = line.split(':', 1)
             if len(parts) == 2:
                  prop_part = parts[0]
                  value_part = parts[1]
                  # Chỉ sửa nếu không có 'T' (dấu hiệu của định dạng datetime chuẩn)
                  # VÀ không phải là VALUE=DATE (vì VALUE=DATE chỉ có ngày, không có giờ)
                  if 'T' not in value_part and 'VALUE=DATE' not in prop_part:
                       match_val = dt_malformed_val_pattern.match(value_part)
                       if match_val:
                            date_part = match_val.group(1)
                            time_part_malformed = match_val.group(2)
                            z_part = match_val.group(3)
                            # Check if time_part_malformed is at least 6 digits for HHMMSS
                            if len(time_part_malformed) >= 6:
                                 # Thử sửa: thêm 'T', lấy 6 chữ số đầu tiên của phần giờ (HHMMSS)
                                 fixed_value = date_part + 'T' + time_part_malformed[:6] + z_part
                                 final_fixed_lines.append(f"{prop_part}:{fixed_value}")
                                 dt_fix_count += 1
                            else:
                                 # Phần giờ quá ngắn, không sửa được theo mẫu này
                                 print(f"Cảnh báo: Không sửa được định dạng DTSTART/DTEND (phần giờ quá ngắn) sau sửa khoảng trắng: '{line}'", file=sys.stderr)
                                 final_fixed_lines.append(line) # Giữ nguyên dòng từ pass 1
                       else:
                            # Không khớp với mẫu định dạng sai đã biết, giữ nguyên
                            final_fixed_lines.append(line) # Giữ nguyên dòng từ pass 1
                  else:
                      # Dòng đã có 'T' hoặc là VALUE=DATE, giả định định dạng ổn (hoặc không thể sửa tự động)
                      final_fixed_lines.append(line) # Giữ nguyên dòng từ pass 1
             else:
                  # Dòng bắt đầu bằng DTSTART/DTEND nhưng không tách thành 2 phần ở dấu hai chấm đầu tiên
                  print(f"Cảnh báo: Dòng bắt đầu bằng DTSTART/DTEND nhưng lỗi định dạng sau sửa khoảng trắng: '{line}'", file=sys.stderr)
                  final_fixed_lines.append(line) # Giữ nguyên dòng từ pass 1
         else:
              final_fixed_lines.append(line) # Các dòng khác (BEGIN, END, SUMMARY, etc.), giữ nguyên từ pass 1


    if space_fix_count > 0 or dt_fix_count > 0:
        print(f"Hoàn tất sửa định dạng file ICS. Đã sửa {space_fix_count} lỗi khoảng trắng sau ':' và {dt_fix_count} lỗi định dạng DTSTART/DTEND.")
    else:
        print("Không tìm thấy lỗi định dạng phổ biến nào cần sửa trong file ICS.")

    # Trả về nội dung đã sửa, đảm bảo có ký tự xuống dòng cuối cùng.
    return '\n'.join(final_fixed_lines) + '\n'

# --- HÀM HỖ TRỢ LẤY SỰ KIỆN ĐÃ TỒN TẠI TỪ GOOGLE CALENDAR (BỔ SUNG) ---
def fetch_google_events_in_range(service, calendar_id, start_year, end_year):
    """
    Fetches all all-day events from calendar_id within the year range [start_year, end_year].
    Returns a set of (date_string (YYYY-MM-DD)) for lookup.
    Handles pagination. Returns an empty set if fetching fails.
    """
    existing_dates_set = set() # Chú ý: Bây giờ chỉ lưu ngày dương dạng chuỗi
    print(f"\nĐang lấy ngày sự kiện đã tồn tại từ lịch Google '{calendar_id}' từ năm {start_year} đến {end_year} để kiểm tra trùng lặp...")

    # Google Calendar API's timeMin và timeMax mong đợi định dạng RFC3339 với múi giờ.
    # Đối với sự kiện cả ngày, lọc theo chuỗi ngày (YYYY-MM-DD) hoạt động tốt.
    # Ta cần chỉ định phạm vi thời gian cho lệnh gọi API.
    # Đặt timeMin là đầu ngày đầu tiên và timeMax là đầu ngày *sau* ngày cuối cùng.
    try:
        start_date_range = date(start_year, 1, 1)
        end_date_range = date(end_year, 12, 31)
        # Sử dụng astimezone(pytz.utc).isoformat() là cách chuẩn để đảm bảo định dạng RFC3339 UTC
        time_min_iso = datetime.combine(start_date_range, datetime.min.time()).astimezone(pytz.utc).isoformat()
        time_max_iso = datetime.combine(end_date_range + timedelta(days=1), datetime.min.time()).astimezone(pytz.utc).isoformat()

    except ValueError as e:
        print(f"Lỗi cấu hình năm (START_YEAR_ICS/END_YEAR_ICS) không hợp lệ: {e}. Không thể lấy sự kiện.", file=sys.stderr)
        return existing_dates_set # Trả về tập rỗng nếu cấu hình năm lỗi

    page_token = None
    processed_fetched_count = 0 # Đếm tổng số sự kiện đã lấy về (bao gồm cả những cái có thể trùng ngày trong Google Calendar nếu SingleEvents=False, nhưng ở đây là True)
    processed_unique_dates = 0 # Đếm số ngày dương duy nhất đã lấy về

    try:
        while True:
            # Gọi API list events với bộ lọc thời gian
            events_result = service.events().list(
                calendarId=calendar_id,
                timeMin=time_min_iso,
                timeMax=time_max_iso,
                singleEvents=True, # Quan trọng: mở rộng các sự kiện định kỳ để kiểm tra từng ngày cụ thể
                orderBy='startTime', # Sắp xếp theo thời gian để pagination hoạt động tốt
                pageToken=page_token,
                # Chỉ yêu cầu các trường cần thiết để giảm tải
                # Chỉ lấy Start time (chỉ date cho all-day) để tạo key ngày
                fields='nextPageToken,items(start)' # Chỉ cần trường 'start'
            ).execute()

            events = events_result.get('items', [])

            if not events:
                # print("Không tìm thấy sự kiện nào trong phạm vi này trong trang hiện tại.") # Debug info
                break # Không còn sự kiện nào trong trang này hoặc không còn trang

            for event in events:
                # Chỉ xem xét các sự kiện 'cả ngày' (những sự kiện có key 'date' trong 'start')
                if 'date' in event.get('start', {}):
                    event_date_str = event['start']['date'] # Định dạngYYYY-MM-DD

                    # Thêm chuỗi ngày dương vào set. Set tự loại bỏ trùng lặp.
                    if event_date_str not in existing_dates_set:
                         existing_dates_set.add(event_date_str)
                         processed_unique_dates += 1

                    processed_fetched_count += 1 # Đếm tổng số item API trả về

            page_token = events_result.get('nextPageToken')
            if not page_token:
                break # Không còn trang kết quả tiếp theo

            # Có thể thêm một khoảng dừng ngắn giữa các lần gọi API lấy trang nếu cần (ít cần hơn khi fetch một range lớn)
            # time.sleep(0.05) # Ví dụ dừng 50ms

    except Exception as e:
        print(f"\nLỗi khi lấy ngày sự kiện đã tồn tại từ Google Calendar: {e}", file=sys.stderr)
        # print(traceback.format_exc(), file=sys.stderr) # Bỏ comment để xem traceback đầy đủ nếu lỗi xảy ra ở đây
        print("Việc kiểm tra trùng lặp có thể không chính xác do lỗi khi lấy dữ liệu lịch.", file=sys.stderr)
        # Trả về tập rỗng để script tiếp tục nhưng có thể gây trùng lặp nếu không lấy được hết
        return set()

    print(f"Hoàn tất lấy dữ liệu. Tổng số ngày dương duy nhất đã lấy sự kiện từ lịch Google: {len(existing_dates_set)}")
    # print(f"(Tổng số item sự kiện lấy về từ API: {processed_fetched_count})") # Debug info
    return existing_dates_set # Trả về tập hợp các chuỗi ngày dương đã có


# --- HÀM NHẬP SỰ KIỆN VÀO GOOGLE CALENDAR (SỬ DỤNG BATCHING VÀ KIỂM TRA TRÙNG LẶP) ---
# Hàm này sẽ đọc từ file ICS_FILE đã được sửa từ file cục bộ
# BỔ SUNG: Kiểm tra sự kiện đã tồn tại trên Google Calendar (chỉ dựa trên ngày) trước khi thêm vào batch
# BỔ SUNG: Thêm tên ngày lễ vào tiêu đề cho ngày đặc biệt
def import_ics_batched(service, calendar_id):
    """
    Đọc sự kiện từ file ICS đã sửa (ICS_FILE), biến đổi tiêu đề, tô màu đỏ các ngày lễ đặc biệt,
    và nhập các sự kiện vào Google Calendar đích (calendar_id) sử dụng batching.
    Trước khi nhập, sẽ kiểm tra các ngày sự kiện đã tồn tại trong lịch Google
    trong phạm vi năm cấu hình (START_YEAR_ICS, END_YEAR_ICS) để tránh tạo trùng lặp
    dựa trên ngày dương.
    Chỉ thêm các sự kiện từ ICS NẾU chúng chưa tồn tại trong lịch Google vào cùng ngày đó.
    """
    if not service:
        print("Không có đối tượng service API, không thể nhập sự kiện.", file=sys.stderr)
        return
    if not calendar_id:
        print("Không có Calendar ID đích, không thể nhập sự kiện.", file=sys.stderr)
        return

    # --- BƯỚC 0: LẤY CÁC NGÀY CỦA SỰ KIỆN ĐÃ TỒN TẠI TRÊN GOOGLE CALENDAR ---
    # Sử dụng hàm mới để lấy set các ngày dương đã có sự kiện trong phạm vi năm đã cấu hình
    existing_google_event_dates_set = fetch_google_events_in_range(service, calendar_id, START_YEAR_ICS, END_YEAR_ICS)

    try:
        # Mở và đọc nội dung file ICS đã được sửa (đảm bảo đã chạy bước fix_ics_format trước đó)
        # Đọc dưới dạng binary ('rb') là cách icalendar thường làm việc tốt nhất
        with open(ICS_FILE, 'rb') as f:
            # Phân tích nội dung file ICS thành các component
            gcal = Calendar.from_ical(f.read())
    except FileNotFoundError:
        print(f"Lỗi đọc file ICS: Không tìm thấy tệp ICS đã sửa '{ICS_FILE}'.", file=sys.stderr)
        print("Hãy chắc chắn rằng bước xử lý file đã thành công trước khi chạy nhập.", file=sys.stderr)
        return
    except Exception as e:
        print(f"Lỗi khi đọc và phân tích tệp ICS '{ICS_FILE}': {e}", file=sys.stderr)
        if hasattr(e, '__cause__') and e.__cause__:
            print(f"Chi tiết lỗi từ icalendar: {e.__cause__}", file=sys.stderr)
        print("Có thể file ICS vẫn còn lỗi định dạng hoặc thư viện icalendar gặp vấn đề.", file=sys.stderr)
        return

    # Lọc ra chỉ các VEVENT (các component biểu diễn sự kiện)
    events_to_import = [component for component in gcal.walk() if component.name == "VEVENT"]
    total_events_ics = len(events_to_import) # Tổng số sự kiện tìm thấy trong file

    if total_events_ics == 0:
        print(f"Không tìm thấy sự kiện nào (VEVENT) trong tệp ICS '{ICS_FILE}' để nhập.")
        return

    # --- KHỞI TẠO BATCHING ---
    batch = service.new_batch_http_request()
    batch_requests_count = 0

    # Biến đếm cho báo cáo tổng kết cuối cùng
    total_skipped_duplicate_count = 0 # Tổng số sự kiện bị bỏ qua do đã tồn tại (dựa trên ngày)
    total_error_processing_count = 0 # Tổng số sự kiện gặp lỗi khi xử lý data từ ICS (trước khi check trùng lặp/thêm batch)
    # total_api_error_count sẽ được cập nhật bởi callback cho lỗi trong batch (callback chỉ in lỗi)
    events_chosen_for_insert = 0 # Đếm số sự kiện đã qua kiểm tra trùng lặp và xử lý data thành công, được thêm vào batch.


    # --- HÀM CALLBACK XỬ LÝ KẾT QUẢ CỦA TỪNG YÊU CẦU TRONG BATCH ---
    # Callback này chỉ xử lý kết quả cho các yêu cầu insert ĐƯỢC CHỌN để thêm vào batch
    # request_id ở đây là số thứ tự idx của sự kiện gốc từ file ICS (dùng khi batch.add)
    def batch_callback(request_id, response, exception):
        # Sử dụng request_id (là idx) để tham chiếu đến sự kiện gốc nếu cần debug
        if exception:
            # Check if it's a 409 Conflict specifically
            is_conflict = False
            if hasattr(exception, 'resp') and exception.resp.status == 409:
                 is_conflict = True

            if is_conflict:
                 # Log the conflict but allow process to continue
                 print(f"\n--- Bỏ qua sự kiện gốc #{request_id} do XUNG ĐỘT (409 Conflict) ---", file=sys.stderr)
                 # Optionally print more details for specific conflict debugging
                 # print(f"Details: {exception}", file=sys.stderr)
                 # if hasattr(exception, 'content'):
                 #     try:
                 #         error_details = json.loads(exception.content)
                 #         print(f"Error Details: {json.dumps(error_details, indent=2)}", file=sys.stderr)
                 #     except:
                 #         print(f"Error Content: {exception.content}", file=sys.stderr)
                 # print("---------------------------------------------------", file=sys.stderr)

            else: # Handle other types of API errors
                 print(f"\n--- Lỗi API (không phải Conflict) khi nhập sự kiện gốc #{request_id} ---", file=sys.stderr)
                 print(f"Exception: {exception}", file=sys.stderr)
                 if hasattr(exception, 'resp') and hasattr(exception.resp, 'status'):
                      print(f"HTTP Status: {exception.resp.status}", file=sys.stderr)
                 if hasattr(exception, 'content'):
                      try:
                          error_details = json.loads(exception.content)
                          print(f"Error Details: {json.dumps(error_details, indent=2)}", file=sys.stderr)
                      except:
                          print(f"Error Content: {exception.content}", file=sys.stderr)
                 print("--------------------------", file=sys.stderr)
        else:
            # Yêu cầu insert thành công. Không cần làm gì đặc biệt ở đây ngoài in debug nếu muốn.
            # print(f"Successfully inserted event gốc #{request_id}") # Bỏ comment nếu muốn thông báo từng cái thành công
            pass

    print(f"\nĐang chuẩn bị nhập {total_events_ics} sự kiện từ file ICS vào lịch có ID '{calendar_id}' (sử dụng batching và kiểm tra trùng lặp chỉ dựa trên ngày)...")
    last_batch_execute_time = time.time()

    # --- BẮT ĐẦU VÒNG LẶP DUYỆT CÁC SỰ KIỆN TỪ FILE ICS ---
    for idx, component in enumerate(events_to_import, start=1):
        raw_summary = str(component.get('summary', ''))
        summary = raw_summary # Khởi tạo summary cuối cùng
        event_color_id = None
        start_date_only = None

        try:
            # --- BƯỚC XỬ LÝ 1: Xử lý dữ liệu thời gian ---
            dtstart_ical = component.get('dtstart')
            if not dtstart_ical:
                 print(f"[{idx}/{total_events_ics}] Bỏ qua sự kiện (tiêu đề gốc: '{raw_summary}') do thiếu DTSTART.", file=sys.stderr)
                 total_error_processing_count += 1
                 continue # Bỏ qua sự kiện này

            dtstart_obj = dtstart_ical.dt # Có thể là date hoặc datetime
            start_date_only = dtstart_obj.date() if isinstance(dtstart_obj, datetime) else dtstart_obj

            # --- BƯỚC XỬ LÝ 2: Xử lý tiêu đề và màu sắc (Bao gồm thêm tên lễ) ---
            # Sử dụng regex để tìm mẫu "DD/MM" hoặc "DD/MMN" từ đầu chuỗi
            match = re.match(r'^(\d{2})\/(\d{2})N?$', raw_summary)
            if match:
                 day_str = match.group(1)
                 month_str = match.group(2)

                 try:
                     day = int(day_str)
                     month = int(month_str)
                     key_day_month = f"{day}/{month}" # Key ví dụ: "1/1", "15/7"

                     # Định dạng tiêu đề mặc định là "Ngày/Tháng AL"
                     summary = f"{day}/{month} AL"

                     # Kiểm tra xem ngày/tháng này có tồn tại trong danh sách các ngày lễ đặc biệt không
                     if key_day_month in special_days:
                         # Nếu là ngày lễ đặc biệt
                         special_day_name = special_days[key_day_month]
                         # Gán colorId
                         event_color_id = '11' # ID màu '11' thường là màu đỏ
                         # Bổ sung tên lễ vào tiêu đề đã định dạng "Ngày/Tháng AL"
                         summary = f"{day}/{month} AL - {special_day_name}" # <--- ĐÃ BỔ SUNG TÊN LỄ

                 except ValueError as e:
                     print(f"[{idx}/{total_events_ics}] Cảnh báo: Lỗi chuyển đổi ngày/tháng từ tiêu đề gốc '{raw_summary}': {e}. Giữ nguyên tiêu đề gốc.", file=sys.stderr)
                     summary = raw_summary # Giữ nguyên tiêu đề gốc nếu lỗi xử lý
                     # Màu sắc vẫn là None
            # else: Nếu tiêu đề gốc KHÔNG khớp với mẫu DD/MM(N), giữ nguyên summary là raw_summary. Màu sắc None.

            # --- BƯỚC XỬ LÝ 3: Tạo khóa ngày dương để kiểm tra trùng lặp ---
            event_date_str = start_date_only.strftime('%Y-%m-%d') if start_date_only else None


        except Exception as e:
            # Bắt lỗi xảy ra trong quá trình xử lý dữ liệu ngày/giờ hoặc tiêu đề của sự kiện này
            print(f"\n--- Lỗi xử lý dữ liệu cho sự kiện [{idx}/{total_events_ics}] (tiêu đề gốc: '{raw_summary}') ---", file=sys.stderr)
            print(f"Exception: {e}", file=sys.stderr)
            # if 'component' in locals() and hasattr(component, 'to_ical'): # Check if component exists and has to_ical method
            #      try:
            #          # Cố gắng in dữ liệu thô của component để debug
            #          print(f"Component raw data:\n{component.to_ical().decode('utf-8')}", file=sys.stderr)
            #      except:
            #          print("Không thể decode dữ liệu thô của component.", file=sys.stderr)
            print("--------------------------", file=sys.stderr)
            total_error_processing_count += 1
            continue # Bỏ qua sự kiện này và chuyển sang sự kiện kế tiếp

        # --- Thêm logging để biết sự kiện nào đang được xử lý ---
        if event_date_str:
             print(f"[{idx}/{total_events_ics}] Đang xử lý ICS event cho ngày {event_date_str} (Tiêu đề: '{summary}')...", file=sys.stdout) # In ra stdout để dễ thấy tiến trình

        # --- BƯỚC 4: KIỂM TRA TRÙNG LẶP TRƯỚC KHI THÊM VÀO BATCH (CHỈ DỰA TRÊN NGÀY) ---
        # Chỉ kiểm tra trùng lặp nếu ngày dương đã được xác định
        if event_date_str and event_date_str in existing_google_event_dates_set:
            # Nếu đã có sự kiện trên ngày dương này trong lịch Google
            print(f"[{idx}/{total_events_ics}] Bỏ qua: Sự kiện vào ngày {event_date_str} có thể đã tồn tại trên lịch Google (dựa trên ngày). Tiêu đề ICS: '{summary}'", file=sys.stderr) # In ra stderr để phân biệt với tiến trình
            total_skipped_duplicate_count += 1
            continue # Bỏ qua sự kiện này và không thêm vào batch

        # --- Nếu không có sự kiện nào trên ngày này, tiếp tục BƯỚC 5: CHUẨM BỊ BODY VÀ THÊM VÀO BATCH ---
        try:
            dtend_ical = component.get('dtend')
            dtend_obj = dtend_ical.dt if dtend_ical else None

            end_date_only = None
            if dtend_obj:
                 # Đối với sự kiện cả ngày từ ICS, dtend thường là ngày tiếp theo của ngày cuối cùng
                 end_date_only = dtend_obj.date() if isinstance(dtend_obj, datetime) else dtend_obj

            event_body = {
                'summary': summary, # <-- Sử dụng biến 'summary' ĐÃ ĐƯỢC Xử LÝ (có thể có tên lễ)
                'description': str(component.get('description', '')), # Mô tả gốc từ ICS
                # Cấu hình nhắc nhở (tắt mặc định, bật popup 10 phút trước - có thể chỉnh)
                'reminders': {'useDefault': False, 'overrides': [{'method': 'popup', 'minutes': 10}]},
                # Định dạng thời gian cho Google Calendar API (LUÔN XỬ LÝ NHƯ SỰ KIỆN CẢ NGÀY)
                'start': {
                    'date': event_date_str, # Sử dụng chuỗi ngày dương đã xác định
                    'timeZone': 'Asia/Ho_Chi_Minh' # Google Calendar sẽ dùng múi giờ này để xác định "ngày"
                },
            }

            # Xử lý DTEND cho sự kiện cả ngày trong Google Calendar API
            # Google API cần ngày kết thúc là ngày *sau* ngày cuối cùng của sự kiện
            if end_date_only:
                 # Nếu DTEND có trong ICS, dùng nó nhưng đảm bảo là ngày sau ngày bắt đầu nếu chỉ là sự kiện 1 ngày
                 end_date_exclusive = end_date_only
                 # Nếu ngày kết thúc trùng hoặc trước ngày bắt đầu, làm cho nó là ngày sau ngày bắt đầu
                 if end_date_exclusive <= start_date_only:
                     end_date_exclusive = start_date_only + timedelta(days=1)
                 event_body['end'] = {
                     'date': end_date_exclusive.strftime('%Y-%m-%d'),
                     'timeZone': 'Asia/Ho_Chi_Minh'
                 }
            else:
                 # Nếu không có DTEND trong ICS, giả định là sự kiện 1 ngày, ngày kết thúc là ngày sau ngày bắt đầu
                 event_body['end'] = {
                     'date': (start_date_only + timedelta(days=1)).strftime('%Y-%m-%d'),
                     'timeZone': 'Asia/Ho_Chi_Minh'
                 }


            # Thêm màu sắc nếu là ngày lễ (đã xác định ở BƯỚC Xử LÝ 2)
            if event_color_id:
                 event_body['colorId'] = event_color_id

        except Exception as e:
            # Bắt lỗi xảy ra khi chuẩn bị event_body (rất hiếm nếu các bước trước thành công)
             print(f"\n--- Lỗi chuẩn bị event body cho sự kiện [{idx}/{total_events_ics}] (tiêu đề cuối: '{summary}') ---", file=sys.stderr)
             print(f"Exception: {e}", file=sys.stderr)
             total_error_processing_count += 1 # Vẫn đếm là lỗi xử lý dữ liệu
             continue # Bỏ qua sự kiện này

        # --- BƯỚC 6: THÊM YÊU CẦU THÊM SỰ KIỆN NÀY VÀO BATCH ---
        # Thêm yêu cầu insert sự kiện vào đối tượng batch.
        # Chỉ những sự kiện KHÔNG trùng lặp và xử lý dữ liệu thành công mới đến được đây.
        batch.add(
            service.events().insert(calendarId=calendar_id, body=event_body),
            # Sử dụng idx làm request_id để theo dõi trong callback (nếu có lỗi API)
             callback=batch_callback,
            request_id=str(idx) # Sử dụng chỉ mục gốc làm ID yêu cầu
        )
        batch_requests_count += 1
        events_chosen_for_insert += 1 # Tăng biến đếm sự kiện ĐƯỢC CHỌN để insert vào batch

        # In thông báo tiến trình thêm vào batch (có thể bỏ comment nếu muốn)
        # print(f"[{idx}/{total_events_ics}] Đã thêm vào batch: '{summary}'") # Bỏ comment để giảm output

        # --- BƯỚC 7: KIỂM TRA VÀ THỰC THI BATCH NẾU ĐẦY ---
        if batch_requests_count >= BATCH_SIZE:
            print(f"Đang thực thi batch (gói {batch_requests_count} sự kiện). Đã xử lý {idx}/{total_events_ics} sự kiện ICS...", file=sys.stdout)
            batch_start_time = time.time()
            required_batch_duration = MIN_DELAY_BETWEEN_BATCHES
            elapsed_since_last_batch_start = batch_start_time - last_batch_execute_time

            if elapsed_since_last_batch_start < required_batch_duration:
                 sleep_duration = required_batch_duration - elapsed_since_last_batch_start
                 print(f"Ngủ {sleep_duration:.4f}s để tuân thủ rate limit giữa các batch.", file=sys.stdout)
                 time.sleep(sleep_duration)

            last_batch_execute_time = time.time()
            try:
                # Thực thi batch. Lệnh này gửi yêu cầu đến Google API.
                # Google sẽ xử lý các yêu cầu trong batch và gọi lại hàm batch_callback cho kết quả của từng yêu cầu con.
                batch.execute()
                # print(f"Batch thực thi hoàn tất trong {time.time() - last_batch_execute_time:.2f}s. Kết quả đang được xử lý bởi callback.") # Debug timing

            except Exception as e:
                # Xử lý lỗi ở cấp độ TOÀN BỘ batch (ví dụ: lỗi xác thực cho toàn bộ batch, lỗi mạng)
                print(f"\nLỗi ở cấp độ Batch khi thực thi gói {idx}/{total_events_ics}: {e}.", file=sys.stderr)
                # Các sự kiện con trong batch này có thể không được nhập. Lỗi được in chi tiết bởi callback nếu nó được gọi.

            # Reset batch và bộ đếm cho gói batch tiếp theo
            batch = service.new_batch_http_request()
            batch_requests_count = 0

    # --- SAU VÒNG LẶP DUYỆT ICS: THỰC THI CÁC YÊU CẦU CÒN LẠI TRONG BATCH CUỐI CÙNG ---
    if batch_requests_count > 0:
        print(f"Đang thực thi batch cuối cùng ({batch_requests_count} sự kiện). Đã xử lý hết {total_events_ics}/{total_events_ics} sự kiện ICS...", file=sys.stdout)
        batch_start_time = time.time()
        required_batch_duration = MIN_DELAY_BETWEEN_BATCHES
        elapsed_since_last_batch_start = batch_start_time - last_batch_execute_time
        if elapsed_since_last_batch_start < required_batch_duration:
             sleep_duration = required_batch_duration - elapsed_since_last_batch_start
             print(f"Ngủ {sleep_duration:.4f}s để tuân thủ rate limit cho batch cuối cùng.", file=sys.stdout)
             time.sleep(sleep_duration)
        last_batch_execute_time = time.time()
        try:
            batch.execute()
            # print(f"Batch cuối cùng thực thi hoàn tất trong {time.time() - last_batch_execute_time:.2f}s.") # Debug timing
        except Exception as e:
            print(f"\nLỗi ở cấp độ Batch khi thực thi gói cuối cùng: {e}.", file=sys.stderr)

    # --- BƯỚC CUỐI CÙNG: TỔNG KẾT QUÁ TRÌNH NHẬP ---
    # total_events_ics: Tổng số sự kiện trong file ICS.
    # len(existing_google_event_dates_set): Số ngày dương duy nhất đã có sự kiện trên Google (đã lấy).
    # total_skipped_duplicate_count: Số sự kiện từ ICS bị bỏ qua vì đã có sự kiện trên cùng ngày (dựa trên ngày).
    # total_error_processing_count: Số sự kiện từ ICS gặp lỗi xử lý dữ liệu ban đầu.
    # events_chosen_for_insert: Số sự kiện từ ICS được chọn để thêm vào batch (sau khi check trùng lặp và xử lý lỗi).

    print(f"\n--- Tổng kết quá trình nhập lịch (Kiểm tra trùng lặp chỉ dựa trên ngày + Batching) ---", file=sys.stdout)
    print(f"Tổng số sự kiện tìm thấy trong tệp ICS: {total_events_ics}", file=sys.stdout)
    print(f"Tổng số ngày dương duy nhất đã lấy sự kiện từ lịch Google để kiểm tra: {len(existing_google_event_dates_set)}", file=sys.stdout)
    print(f"Số sự kiện từ ICS bị bỏ qua do đã tồn tại sự kiện trên cùng ngày trong lịch Google: {total_skipped_duplicate_count}", file=sys.stdout)
    print(f"Số sự kiện từ ICS gặp lỗi xử lý dữ liệu ban đầu (không thêm vào batch): {total_error_processing_count}", file=sys.stdout)

    # Số sự kiện được chọn để thêm vào batch = Tổng sự kiện ICS - Bỏ qua trùng lặp - Lỗi xử lý ban đầu
    # Biến events_chosen_for_insert đã đếm chính xác số này trong vòng lặp.
    print(f"Số sự kiện từ ICS được chọn để cố gắng nhập vào Google Calendar: {events_chosen_for_insert}", file=sys.stdout)

    # Lưu ý: total_error_processing_count đếm lỗi trước khi thêm vào batch.
    # Callback batch_callback in ra lỗi API xảy ra TRONG quá trình thực thi batch (bao gồm cả lỗi 409).
    # Số lượng sự kiện được nhập thành công thực tế trên Google Calendar
    # sẽ xấp xỉ events_chosen_for_insert trừ đi tổng số lỗi API được báo cáo bởi callback.

    print("\nQuy trình kiểm tra và nhập sự kiện hoàn thành.", file=sys.stdout)


# --- KHỐI THỰC THI CHÍNH CỦA SCRIPT ---
# Đây là điểm bắt đầu khi bạn chạy file Python trực tiếp
if __name__ == '__main__':
    print("--- BẮT ĐẦU QUY TRÌNH NHẬP LỊCH ÂM TỪ FILE CỤC BỘ (BATCHING + KIỂM TRA TRÙNG LẶP) ---", file=sys.stdout)

    # Bước 1: Đọc và sửa lỗi định dạng file ICS TỪ FILE CỤC BỘ
    print(f"\n--- BƯỚC 1: ĐỌC VÀ SỬA FILE ICS CỤC BỘ '{LOCAL_ICS_INPUT_FILE}' ---", file=sys.stdout)
    local_file_exists = os.path.exists(LOCAL_ICS_INPUT_FILE)
    if local_file_exists:
        try:
            # Mở file ở chế độ đọc ('r') với encoding 'utf-8'
            with open(LOCAL_ICS_INPUT_FILE, 'r', encoding='utf-8') as f:
                 raw_text = f.read()
            # Sửa lỗi định dạng bằng hàm đã có
            fixed_text = fix_ics_format(raw_text)
            # Lưu nội dung đã sửa vào file tạm (ICS_FILE)
            # Mở file ở chế độ ghi ('w') với encoding 'utf-8'
            with open(ICS_FILE, 'w', encoding='utf-8') as f:
                 f.write(fixed_text)
            print(f"Đã đọc file '{LOCAL_ICS_INPUT_FILE}' và lưu nội dung đã sửa vào '{ICS_FILE}'.", file=sys.stdout)
            file_processed_successfully = True # Đánh dấu xử lý file thành công
        except FileNotFoundError:
             # Should not happen because we checked os.path.exists, but good practice
             print(f"Lỗi: Không tìm thấy tệp ICS cục bộ '{LOCAL_ICS_INPUT_FILE}'. Vui lòng kiểm tra tên file và đường dẫn.", file=sys.stderr)
             file_processed_successfully = False
        except Exception as e:
             print(f"Lỗi khi đọc hoặc sửa file ICS cục bộ '{LOCAL_ICS_INPUT_FILE}': {e}", file=sys.stderr)
             print("Đảm bảo file không bị hỏng và có quyền đọc.", file=sys.stderr)
             file_processed_successfully = False
    else:
        print(f"Lỗi: Không tìm thấy tệp ICS cục bộ '{LOCAL_ICS_INPUT_FILE}' trong thư mục hiện tại.", file=sys.stderr)
        print("Hãy chắc chắn file ICS đầu vào nằm cùng thư mục với script hoặc cung cấp đường dẫn đầy đủ.", file=sys.stderr)
        file_processed_successfully = False

    # Chỉ tiếp tục nếu bước 1 (xử lý file ICS cục bộ) thành công
    if file_processed_successfully:
        # Bước 2: Xác thực với Google API để có quyền truy cập dịch vụ
        print("\n--- BƯỚC 2: XÁC THỰC GOOGLE API ---", file=sys.stdout)
        google_service = get_service()
        if google_service:
            # Nếu bước 2 thành công (có đối tượng service), chuyển sang bước 3
            # Bước 3: Tìm hoặc tạo lịch Google Calendar đích
            print("\n--- BƯỚC 3: TÌM HOẶC TẠO LỊCH ĐÍCH ---", file=sys.stdout)
            target_calendar_id = get_or_create_calendar(google_service)
            if target_calendar_id:
                # Nếu bước 3 thành công (có ID lịch), chuyển sang bước 4
                # Bước 4: Nhập các sự kiện từ file ICS đã sửa vào lịch đích (sử dụng Batching và Kiểm tra trùng lặp)
                print(f"\n--- BƯỚC 4: NHẬP LỊCH VÀO GOOGLE CALENDAR (BATCHING + KIỂM TRA TRÙNG LẶP) TỪ FILE '{ICS_FILE}' ---", file=sys.stdout)
                # Gọi hàm nhập batching, truyền đối tượng service và ID lịch đích.
                # Hàm này sẽ đọc từ ICS_FILE và tự động lấy sự kiện Google để kiểm tra trùng lặp.
                import_ics_batched(google_service, target_calendar_id) # <-- Gọi hàm nhập batching đã chỉnh sửa
                # Sau khi hàm nhập kết thúc
                print("\nQuy trình nhập sự kiện hoàn thành.", file=sys.stdout)
            else:
                # Nếu bước 3 lỗi
                print("\nKết thúc quy trình: Không thể tạo hoặc lấy lịch Google Calendar đích. Vui lòng kiểm tra lại quyền truy cập và cấu hình.", file=sys.stderr)
        else:
            # Nếu bước 2 lỗi
            print("\nKết thúc quy trình: Lỗi xác thực Google API. Không thể kết nối tới dịch vụ. Vui lòng kiểm tra lại credentials.json và kết nối mạng.", file=sys.stderr)
    else:
        # Nếu bước 1 lỗi
        print("\nKết thúc quy trình: Xử lý file ICS cục bộ không thành công. Không có dữ liệu nguồn để nhập.", file=sys.stderr)

    print("\n--- KẾT THÚC TOÀN BỘ QUY TRÌNH SCRIPT ---", file=sys.stdout)
