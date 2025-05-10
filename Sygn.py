# -*- coding: utf-8 -*-
# Import các thư viện cần thiết
from datetime import date, datetime, timedelta
import os
import time
import requests # Để tải file từ URL (cần pip install requests)
import re # Để xử lý biểu thức chính quy (có sẵn trong Python)

# Import các thư viện Google API
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow # Cần pip install google-auth-oauthlib
from googleapiclient.discovery import build # Cần pip install google-api-python-client
from google.auth.transport.requests import Request # Cần pip install google-auth

# Import thư viện xử lý file ICS
from icalendar import Calendar # Cần pip install icalendar

# Import thư viện xử lý múi giờ
from pytz import timezone # Cần pip install pytz


# --- HẰNG SỐ CẤU HÌNH ---
SCOPES = ['https://www.googleapis.com/auth/calendar'] # Scope cần thiết để truy cập và quản lý Google Calendar

# Cấu hình nguồn file ICS
ICS_URL = 'https://raw.githubusercontent.com/thuanducbh/lich-am-vn/refs/heads/main/am_lich_all_years.ics' # URL tới file ICS nguồn
ICS_FILE = 'am_lich_all_years_fixed.ics' # Tên file sẽ lưu nội dung ICS đã được tải về và sửa lỗi định dạng

# Cấu hình lịch Google Calendar đích
CALENDAR_NAME = 'Lịch Âm VN' # Tên lịch trên Google Calendar mà bạn muốn nhập sự kiện vào
VIETNAM_TZ = timezone('Asia/Ho_Chi_Minh') # Múi giờ cho lịch (chọn múi giờ Việt Nam)

# Khoảng trễ tối thiểu (giây) giữa các lệnh gọi API để tuân thủ giới hạn 600/phút (10/giây).
# Đặt là 1/9 giây để đảm bảo an toàn (tối đa khoảng 9 lệnh/giây).
MIN_DELAY_BETWEEN_CALLS = 1.0 / 9.0

# --- DANH SÁCH CÁC NGÀY LỄ ĐẶC BIỆT (LỊCH ÂM) ---
# Dictionary ánh xạ "ngày/tháng âm lịch" sang tên ngày lễ.
# Sử dụng để xác định ngày lễ và gán màu sắc (KHÔNG thêm tên lễ vào tiêu đề trong bản này).
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
# Bạn có thể chỉnh sửa dictionary này nếu cần thêm/bớt ngày lễ hoặc sửa tên. Tên lễ ở đây chỉ dùng cho mục đích debug nếu cần.


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
            print(f"Cảnh báo: Lỗi đọc token.json - {e}. Sẽ tiến hành xác thực mới.")
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
                print(f"Lỗi làm mới token: {e}. Cần xác thực lại từ đầu.")
                creds = None # Reset creds nếu làm mới lỗi
        else:
             # Token không có refresh_token (ví dụ: chỉ là access token tạm thời) hoặc lỗi khác khiến token không hợp lệ
             print("Token không hợp lệ hoặc không thể làm mới, cần xác thực lại từ đầu.")
             creds = None # Reset creds

    # Bước 3: Nếu chưa có credential hợp lệ (lần đầu chạy hoặc xác thực lại), thực hiện luồng xác thực mới
    if not creds or not creds.valid:
        if os.path.exists('credentials.json'):
            print("Đang tiến hành xác thực mới từ credentials.json...")
            try:
                # Khởi tạo luồng OAuth 2.0 từ file client secrets
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                # Chạy server cục bộ để lắng nghe kết quả xác thực từ trình duyệt
                # Điều này yêu cầu máy chạy script có thể mở trình duyệt và kết nối lại đến port ngẫu nhiên.
                # Nếu chạy trên môi trường không có GUI/trình duyệt (như Termux không có trình duyệt ảo, máy chủ SSH chỉ console),
                # bạn có thể cần dùng run_console() hoặc phương thức xác thực thủ công khác.
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES) # <-- Đã sửa indent ở đây
                creds = flow.run_local_server(port=0) # <-- Đã sửa indent ở đây
                print("Xác thực thành công.")

            except FileNotFoundError:
                 print("Lỗi: Không tìm thấy tệp credentials.json!")
                 print("Vui lòng tải file credentials.json từ Google Cloud Console và đặt cùng thư mục với script.")
                 return None
            except Exception as e:
                 print(f"Lỗi trong quá trình xác thực mới: {e}")
                 print("Đảm bảo bạn đã cài google-auth-oauthlib và kết nối mạng ổn định.")
                 return None
        else:
            print("Lỗi: Không tìm thấy tệp credentials.json! Cần file này để xác thực lần đầu nếu token.json không tồn tại hoặc lỗi.")
            return None

    # Bước 4: Lưu credential (có thể đã được làm mới hoặc mới tạo) vào file token.json cho lần chạy sau
    try:
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
        # print("Đã lưu token.json thành công cho lần chạy sau.")
    except Exception as e:
         print(f"Cảnh báo: Không thể lưu token.json - {e}. Token sẽ không được lưu cho lần chạy sau.")

    # Bước 5: Xây dựng và trả về đối tượng service cho Google Calendar API
    try:
        service = build('calendar', 'v3', credentials=creds)
        # Có thể thêm một lệnh gọi API đơn giản ở đây để kiểm tra kết nối nếu muốn, ví dụ:
        # service.calendarList().list(maxResults=1).execute()
        # print("Đã kết nối tới Google Calendar API.")
        return service
    except Exception as e:
        print(f"Lỗi khi xây dựng service hoặc kết nối API: {e}")
        print("Đảm bảo bạn đã cài google-api-python-client và kết nối mạng ổn định.")
        return None

# --- HÀM TÌM HOẶC TẠO LỊCH GOOGLE ---
def get_or_create_calendar(service):
    """
    Tìm lịch Google Calendar theo tên đã cấu hình (CALENDAR_NAME).
    Nếu không tìm thấy, tạo một lịch mới với tên đó và múi giờ Việt Nam.
    Trả về ID của lịch hoặc None nếu lỗi.
    """
    if not service:
        print("Không có đối tượng service API, không thể tìm hoặc tạo lịch.")
        return None

    try:
        print(f"Đang tìm lịch Google Calendar có tên '{CALENDAR_NAME}'...")
        page_token = None
        # API list calendarList có thể trả về kết quả theo trang (nếu số lượng lịch lớn hơn 100)
        while True:
            # Lấy danh sách các lịch của người dùng (lấy từng trang)
            calendar_list = service.calendarList().list(pageToken=page_token).execute()
            calendars = calendar_list.get('items', [])

            # Duyệt qua danh sách lịch trong trang hiện tại để tìm lịch theo tên
            for cal in calendars:
                # So sánh summary (tên lịch) với tên đã cấu hình
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
        print(f"Lỗi khi tìm hoặc tạo lịch Google Calendar: {e}")
        print("Đảm bảo tài khoản Google đã xác thực có quyền xem và quản lý lịch.")
        return None

# --- HÀM SỬA LỖI ĐỊNH DẠNG FILE ICS ---
def fix_ics_format(raw_text):
    """
    Cố gắng sửa các lỗi định dạng phổ biến trong nội dung ICS mà thư viện icalendar có thể khó xử lý:
    - Loại bỏ khoảng trắng thừa sau dấu hai chấm (:) ở các dòng thuộc tính cơ bản (ví dụ: BEGIN: VCALENDAR -> BEGIN:VCALENDAR).
    - Cố gắng sửa định dạng DTSTART/DTEND sai phổ biến (ví dụ: MicheleMMDD0000000Z -> MicheleMMDDTHHMMSSZ).
    Hàm này là một biện pháp tạm thời để làm sạch dữ liệu nguồn nếu nó không tuân thủ chuẩn iCalendar nghiêm ngặt.
    Nó không phải là một trình phân tích cú pháp iCalendar hoàn chỉnh và có thể không sửa được tất cả các loại lỗi.
    """
    lines = raw_text.strip().splitlines() # Xóa khoảng trắng đầu/cuối toàn bộ nội dung và tách thành các dòng riêng lẻ
    fixed_lines = [] # Danh sách để lưu các dòng đã được sửa

    # Biểu thức chính quy để cố gắng bắt các dòng thuộc tính: Tên (tham số tùy chọn) : Giá trị
    # Mục tiêu là tìm tên thuộc tính, phần tham số tùy chọn, dấu hai chấm, và phần còn lại (giá trị).
    # Pattern: ^(TÊN_THUỘC_TÍNH)(;THAM_SỐ)?:(PHẦN_CÒN_LẠI)
    prop_line_pattern = re.compile(r'^([A-Z-]+)(;[^:]*)?:(.*)$')

    # Biểu thức chính quy để tìm giá trị của DTSTART/DTEND có vẻ sai, dạng MicheleMMDD theo sau bởi 7 chữ số và có thể có 'Z'.
    # Ví dụ: 190102200000000Z. Chuẩn phải là MicheleMMDDTHHMMSSZ (chỉ 6 chữ số giờ, có 'T').
    # Mẫu bắt: (\d{8}) là phần ngày, (\d{7}) là 7 chữ số sau ngày, (Z?) là ký tự Z tùy chọn.
    dt_malformed_val_pattern = re.compile(r'^(\d{8})(\d{7})(Z?)$')

    print("Đang tiến hành sửa lỗi định dạng cơ bản trong nội dung ICS...")
    space_fix_count = 0 # Đếm số lỗi khoảng trắng đã sửa
    dt_fix_count = 0 # Đếm số lỗi định dạng DT đã sửa

    # Pass 1: Duyệt qua các dòng để sửa lỗi khoảng trắng sau dấu hai chấm
    for line in lines:
        stripped_line = line.strip() # Xóa khoảng trắng ở đầu và cuối mỗi dòng
        if not stripped_line: # Bỏ qua các dòng trống sau khi strip
            continue

        # Thử khớp dòng với mẫu thuộc tính cơ bản
        match = prop_line_pattern.match(stripped_line)
        if match:
            # Nếu dòng khớp với cấu trúc thuộc tính
            prop_name = match.group(1) # Lấy tên thuộc tính (ví dụ: "BEGIN", "SUMMARY", "DTSTART")
            prop_params = match.group(2) or '' # Lấy phần tham số (nếu có), mặc định là chuỗi rỗng
            # Phần còn lại của dòng, bao gồm cả dấu hai chấm ban đầu và giá trị.
            # Ta cần tìm lại vị trí chính xác của dấu hai chấm sau prop_name và prop_params
            colon_index_after_params = stripped_line.find(':', len(prop_name) + len(prop_params))

            # Kiểm tra nếu tìm thấy dấu hai chấm và có ký tự ngay sau nó
            if colon_index_after_params != -1 and colon_index_after_params + 1 < len(stripped_line):
                 # Kiểm tra nếu ký tự ngay sau dấu hai chấm là khoảng trắng
                 if stripped_line[colon_index_after_params + 1] == ' ':
                     # Nếu có khoảng trắng, xây dựng lại dòng bằng cách loại bỏ khoảng trắng đó
                     # Lấy phần từ đầu đến dấu hai chấm (bao gồm cả dấu hai chấm)
                     # Nối với phần còn lại của dòng BẮT ĐẦU từ ký tự sau khoảng trắng
                     line_after_space_fix = stripped_line[:colon_index_after_params + 1] + stripped_line[colon_index_after_params + 2:]
                     fixed_lines.append(line_after_space_fix) # Thêm dòng đã sửa vào danh sách
                     space_fix_count += 1 # Tăng số đếm lỗi khoảng trắng đã sửa
                     # print(f"Đã sửa khoảng trắng: '{stripped_line}' -> '{fixed_lines[-1]}'") # Có thể bỏ comment để debug

                 else:
                     # Nếu không có khoảng trắng sau dấu hai chấm, giữ nguyên dòng (đã strip)
                     fixed_lines.append(stripped_line)
                     # print(f"Không cần sửa khoảng trắng: '{stripped_line}'") # Có thể bỏ comment để debug
            else:
                # Trường hợp dòng khớp mẫu nhưng không tìm thấy dấu hai chấm ở vị trí mong đợi (lỗi định dạng bất thường khác?)
                # Giữ nguyên dòng đã strip trong pass này
                 fixed_lines.append(stripped_line)

        else:
            # Các dòng không khớp với mẫu thuộc tính cơ bản (ví dụ: các dòng tiếp theo của thuộc tính bị gấp dòng, comment)
            # Với phương pháp sửa lỗi đơn giản này, ta giữ nguyên nội dung của chúng sau khi strip
            fixed_lines.append(stripped_line)
            # print(f"Giữ nguyên (không khớp mẫu thuộc tính): '{stripped_line}'") # Có thể bỏ comment để debug

    # Pass 2: Duyệt qua các dòng đã được sửa khoảng trắng để sửa định dạng DTSTART/DTEND sai
    # Tạo danh sách mới để lưu kết quả sau pass 2
    temp_lines = []

    for line in fixed_lines: # Duyệt qua danh sách các dòng đã được xử lý khoảng trắng
        # Chỉ xử lý các dòng đã sửa bắt đầu bằng "DTSTART:" hoặc "DTEND:"
        if line.startswith('DTSTART:') or line.startswith('DTEND:'):
            # Tách dòng thành 2 phần: Tên thuộc tính (+ params) và Giá trị, chỉ tách ở dấu hai chấm ĐẦU TIÊN
            parts = line.split(':', 1)
            if len(parts) == 2: # Đảm bảo tách được thành 2 phần
                 prop_part = parts[0] # Phần tên thuộc tính, có thể bao gồm tham số (ví dụ: "DTSTART;VALUE=DATE")
                 value_part = parts[1] # Phần giá trị (ví dụ: "190102200000000Z" hoặc "20250101")

                 # Kiểm tra nếu phần giá trị KHÔNG chứa ký tự 'T' (dấu hiệu của định dạng datetime chuẩn)
                 # VÀ phần giá trị khớp với mẫu định dạng sai (MicheleMMDD theo sau bởi 7 chữ số + Z tùy chọn)
                 if 'T' not in value_part:
                      match_val = dt_malformed_val_pattern.match(value_part)
                      if match_val:
                           # Nếu khớp với mẫu định dạng sai
                           date_part = match_val.group(1) # Lấy phần ngày MicheleMMDD
                           time_part_malformed = match_val.group(2) # Lấy phần 7 chữ số sau ngày
                           z_part = match_val.group(3) # Lấy ký tự Z nếu có

                           # Thử sửa: thêm 'T' vào sau ngày, lấy 6 chữ số đầu tiên của phần giờ (HHMMSS)
                           if len(time_part_malformed) >= 6:
                               fixed_value = date_part + 'T' + time_part_malformed[:6] + z_part
                               temp_lines.append(f"{prop_part}:{fixed_value}") # Thêm dòng đã sửa định dạng vào danh sách
                               dt_fix_count += 1 # Tăng số đếm lỗi DT đã sửa
                               # print(f"Đã sửa định dạng DT: '{line}' -> '{temp_lines[-1]}'") # Có thể bỏ comment để debug
                           else:
                               # Nếu phần giờ không đủ 6 chữ số, không sửa được
                               temp_lines.append(line) # Giữ nguyên dòng gốc
                               # print(f"Cảnh báo: Không sửa được định dạng DTSTART/DTEND (phần giờ quá ngắn): '{line}'") # Có thể bỏ comment để debug
                      else:
                          # Không khớp với mẫu định dạng DT sai đã biết (ví dụ: là VALUE=DATE), giữ nguyên
                          temp_lines.append(line)
                 else:
                     # Dòng đã có 'T', giả định là định dạng datetime chuẩn hoặc VALUE=DATE đã có T.
                     temp_lines.append(line) # Giữ nguyên
            else:
                 # Trường hợp dòng bắt đầu bằng DTSTART/DTEND nhưng không tách thành 2 phần ở dấu hai chấm đầu tiên (lỗi định dạng bất thường khác?)
                 temp_lines.append(line) # Giữ nguyên

        else:
            # Các dòng khác không phải DTSTART/DTEND, giữ nguyên
            temp_lines.append(line)

    # In báo cáo tóm tắt về việc sửa lỗi
    if space_fix_count > 0 or dt_fix_count > 0:
        print(f"Hoàn tất sửa định dạng file ICS. Đã sửa {space_fix_count} lỗi khoảng trắng sau ':' và {dt_fix_count} lỗi định dạng DTSTART/DTEND.")
    else:
        print("Không tìm thấy lỗi định dạng phổ biến nào cần sửa trong file ICS.")


    # Cuối cùng, trả về nội dung đã sửa, đảm bảo có ký tự xuống dòng cuối cùng.
    # icalendar thường hoạt động tốt nhất khi file kết thúc bằng một dòng trống hoặc ký tự xuống dòng.
    return '\n'.join(temp_lines) + '\n'


# --- HÀM TẢI VÀ SỬA FILE ICS ---
def download_and_fix_ics():
    """
    Tải nội dung file ICS từ URL đã cấu hình, gọi hàm sửa lỗi định dạng,
    và lưu nội dung đã sửa vào một file cục bộ.
    Trả về True nếu thành công, False nếu lỗi.
    """
    print(f"Đang tải file ICS từ URL: {ICS_URL}")
    try:
        # Thực hiện HTTP GET request để tải nội dung file từ URL
        response = requests.get(ICS_URL)
        # Kiểm tra mã trạng thái HTTP. Nếu không thành công (ví dụ: 404, 500), ném exception.
        response.raise_for_status()

        # Lấy nội dung file dưới dạng văn bản (string).
        # response.text cố gắng tự động phát hiện encoding.
        raw_text = response.text

        # Kiểm tra nhanh xem nội dung tải về có rỗng hoặc chỉ chứa khoảng trắng không
        if not raw_text or not raw_text.strip():
            print("Lỗi tải file ICS: Nội dung tải về từ URL file ICS bị rỗng hoặc không có dữ liệu.")
            return False

        # Gọi hàm sửa lỗi định dạng để xử lý nội dung vừa tải về
        fixed_text = fix_ics_format(raw_text)

        # Ghi nội dung đã sửa vào file cục bộ.
        # Sử dụng chế độ ghi ('w') sẽ tạo file nếu chưa có hoặc ghi đè nếu đã tồn tại.
        # Chỉ định encoding là 'utf-8' để đảm bảo các ký tự tiếng Việt được lưu đúng.
        with open(ICS_FILE, 'w', encoding='utf-8') as f:
            f.write(fixed_text)

        print(f"Đã tải xuống và lưu file ICS đã chỉnh sửa thành công vào '{ICS_FILE}'.")
        return True

    except requests.exceptions.RequestException as e:
        # Bắt các lỗi cụ thể liên quan đến request HTTP (mạng, server lỗi, timeout)
        print(f"Lỗi khi tải file ICS từ URL: {e}")
        print("Kiểm tra lại địa chỉ URL và kết nối mạng của bạn.")
        return False
    except Exception as e:
        # Bắt các loại lỗi khác có thể xảy ra trong quá trình này (ví dụ: lỗi ghi file)
        print(f"Lỗi khác trong quá trình tải và sửa file ICS: {e}")
        return False

# --- HÀM NHẬP SỰ KIỆN VÀO GOOGLE CALENDAR ---
def import_ics_rate_limited(service, calendar_id):
    """
    Đọc sự kiện từ file ICS đã sửa (ICS_FILE), biến đổi tiêu đề sang dạng "Ngày/Tháng AL",
    tô màu đỏ các ngày lễ đặc biệt (không thêm tên lễ vào tiêu đề),
    và nhập các sự kiện vào Google Calendar đích (calendar_id) có kiểm soát tốc độ
    để tuân thủ giới hạn API.

    LƯU Ý QUAN TRỌNG: Hàm này KHÔNG kiểm tra sự kiện trùng lặp dựa trên nội dung lịch Google hiện có.
                      Mỗi lần chạy sẽ nhập lại TẤT CẢ sự kiện từ file ICS.
                      Chạy lại nhiều lần với cùng file ICS sẽ tạo ra các sự kiện trùng lặp trong lịch Google.
                      Nếu bạn cần tránh trùng lặp, bạn cần tự xóa lịch cũ trước khi chạy lại,
                      hoặc triển khai logic kiểm tra trùng lặp phức tạp hơn (đã thảo luận trước đây).
    """
    if not service:
        print("Không có đối tượng service API, không thể nhập sự kiện.")
        return
    if not calendar_id:
        print("Không có Calendar ID đích, không thể nhập sự kiện.")
        return

    try:
        # Mở và đọc nội dung file ICS đã được sửa (đảm bảo đã chạy download_and_fix_ics trước đó)
        # Đọc dưới dạng binary ('rb') là cách icalendar thường làm việc tốt nhất với các loại encoding khác nhau.
        with open(ICS_FILE, 'rb') as f:
            # Phân tích nội dung file ICS thành các component sử dụng thư viện icalendar.
            gcal = Calendar.from_ical(f.read())

    except FileNotFoundError:
        print(f"Lỗi đọc file ICS: Không tìm thấy tệp ICS đã sửa '{ICS_FILE}'.")
        print("Hãy chắc chắn rằng bước tải xuống và sửa file đã thành công trước khi chạy nhập.")
        return
    except Exception as e:
        print(f"Lỗi khi đọc và phân tích tệp ICS '{ICS_FILE}': {e}")
        # In thêm chi tiết lỗi từ icalendar nếu có (ví dụ: nếu file vẫn còn lỗi sau khi sửa)
        if hasattr(e, '__cause__') and e.__cause__:
             print(f"Chi tiết lỗi từ icalendar: {e.__cause__}")
        print("Có thể file ICS vẫn còn lỗi định dạng sau khi sửa hoặc thư viện icalendar gặp vấn đề khác trong quá trình phân tích.")
        return

    # Lọc ra chỉ các VEVENT (các component biểu diễn sự kiện) từ toàn bộ nội dung ICS đã đọc
    events_to_import = [component for component in gcal.walk() if component.name == "VEVENT"]
    total_events = len(events_to_import) # Tổng số sự kiện tìm thấy trong file

    # Kiểm tra nếu không tìm thấy sự kiện nào để nhập
    if total_events == 0:
        print(f"Không tìm thấy sự kiện nào (VEVENT) trong tệp ICS '{ICS_FILE}' để nhập.")
        return

    imported_count = 0 # Đếm số sự kiện đã nhập thành công
    error_count = 0 # Đếm số sự kiện gặp lỗi hoặc bị bỏ qua trong quá trình xử lý

    print(f"Đang chuẩn bị nhập {total_events} sự kiện vào lịch có ID '{calendar_id}' (có kiểm soát tốc độ, biến đổi tiêu đề & tô màu ngày lễ)...")

    last_call_time = time.time() # Ghi lại thời gian của lệnh gọi API cuối cùng để tính khoảng trễ cho Rate Limiting

    # --- BẮT ĐẦU VÒNG LẶP DUYỆT VÀ NHẬP TỪNG SỰ KIỆN ---
    for idx, component in enumerate(events_to_import, start=1):
        # --- BƯỚC 1: BIẾN ĐỔI TIÊU ĐỀ SANG "Ngày/Tháng AL" VÀ XÁC ĐỊNH MÀU SẮC CHO NGÀY LỄ ---
        # Lấy tiêu đề gốc từ component sự kiện, mặc định là chuỗi rỗng nếu không có SUMMARY
        raw_summary = str(component.get('summary', ''))

        # Khởi tạo tiêu đề cuối cùng ban đầu bằng tiêu đề gốc (dùng nếu không khớp mẫu ngày âm lịch)
        summary = raw_summary
        # Khởi tạo colorId mặc định là None (sẽ dùng màu mặc định của lịch Google)
        event_color_id = None

        # Sử dụng biểu thức chính quy để tìm mẫu "Ngày X tháng Y âm lịch" trong tiêu đề gốc
        # (\d{1,2}) sẽ bắt 1 hoặc 2 chữ số cho ngày (group 1) và tháng (group 2)
        match = re.search(r'Ngày (\d{1,2}) tháng (\d{1,2}) âm lịch', raw_summary)

        if match:
            # Nếu tiêu đề gốc khớp với mẫu ngày âm lịch
            day_str = match.group(1) # Lấy chuỗi số ngày (ví dụ: "1" hoặc "23")
            month_str = match.group(2) # Lấy chuỗi số tháng (ví dụ: "1" hoặc "12")

            # Vẫn cần loại bỏ số 0 ở đầu để tạo key cho dictionary ngày lễ
            day = day_str.lstrip("0")
            month = month_str.lstrip("0")
            key_day_month = f"{day}/{month}" # Tạo key ví dụ: "1/1", "15/7", "23/12"

            # ĐẶT TIÊU ĐỀ SỰ KIỆN THEO ĐỊNH DẠNG "Ngày/Tháng AL" (KHÔNG THÊM TÊN LỄ)
            summary = f"{day}/{month} AL" # <--- Định dạng tiêu đề theo yêu cầu mới

            # Kiểm tra xem ngày/tháng này có tồn tại trong danh sách các ngày lễ đặc biệt không
            if key_day_month in special_days:
                # Nếu là ngày lễ đặc biệt, GÁN colorId (NHƯNG KHÔNG THÊM TÊN LỄ VÀO TIÊU ĐỀ)
                event_color_id = '11'  # Gán ID màu '11' (thường là màu đỏ trong Google Calendar) cho sự kiện này

            # else: Nếu tiêu đề gốc khớp mẫu ngày âm lịch nhưng không phải ngày lễ trong dictionary,
            # thì summary sẽ là "Ngày/Tháng AL" và event_color_id vẫn là None (màu mặc định).

        # else: Nếu tiêu đề gốc KHÔNG khớp với mẫu "Ngày X tháng Y âm lịch" (ví dụ: "Tết Dương lịch 1/1"),
        # thì tiêu đề sự kiện vẫn giữ nguyên giá trị của raw_summary ban đầu, và event_color_id vẫn là None.
        # --- KẾT THÚC BƯỚC 1 ---


        # --- BƯỚC 2: XỬ LÝ DỮ LIỆU THỜI GIAN VÀ CHUẨN BỊ BODY CHO API ---
        try:
            # Lấy thời gian bắt đầu (DTSTART) từ component sự kiện
            dtstart_ical = component.get('dtstart')
            # Kiểm tra nếu sự kiện không có DTSTART (lỗi dữ liệu), bỏ qua sự kiện này
            if not dtstart_ical:
                 print(f"[{idx}/{total_events}] Bỏ qua sự kiện (tiêu đề gốc: '{raw_summary}') do thiếu DTSTART.")
                 error_count += 1
                 continue # Chuyển sang xử lý sự kiện kế tiếp trong vòng lặp

            dtstart_obj = dtstart_ical.dt # Lấy đối tượng date hoặc datetime từ DTSTART component

            # Lấy thời gian kết thúc (DTEND) từ component sự kiện, thuộc tính này có thể không có
            dtend_ical = component.get('dtend')
            dtend_obj = dtend_ical.dt if dtend_ical else None # Lấy đối tượng date hoặc datetime nếu có DTEND, nếu không thì là None

            # Chuẩn bị dictionary body dữ liệu để gửi đến Google Calendar API khi tạo sự kiện mới
            event_body = {
                'summary': summary, # <-- Sử dụng biến 'summary' đã được xử lý (đã định dạng "Ngày/Tháng AL")
                'description': str(component.get('description', '')), # Lấy mô tả sự kiện, mặc định là chuỗi rỗng nếu không có DESCRIPTION
                # Cấu hình nhắc nhở cho sự kiện
                'reminders': {
                    'useDefault': False, # Tắt chế độ sử dụng nhắc nhở mặc định của lịch Google
                    'overrides': [
                        {'method': 'popup', 'minutes': 10}, # Thêm một nhắc nhở dạng popup hiển thị trước thời gian sự kiện 10 phút
                        # {'method': 'email', 'minutes': 30}, # Có thể thêm nhắc nhở dạng email nếu muốn, bỏ dấu comment nếu sử dụng
                    ],
                },
                # Thêm trường minh bạch (Transparency) - 'opaque' (Mặc định, đánh dấu là bận) hoặc 'transparent' (đánh dấu là rảnh)
                # 'transparency': 'transparent', # Ví dụ: Có thể bỏ dấu comment dòng này nếu bạn muốn các sự kiện lịch âm hiển thị là "Rảnh" trên lịch Google
            }

            # Thêm trường colorId vào event_body nếu nó đã được xác định trong Bước 1 (chỉ cho các ngày lễ đặc biệt)
            if event_color_id:
                 event_body['colorId'] = event_color_id # <-- Thêm màu sắc vào body request nếu là ngày lễ

            # --- Định dạng thời gian bắt đầu (start) cho Google Calendar API ---
            # Google API yêu cầu định dạng khác nhau cho sự kiện CẢ NGÀY và sự kiện CÓ GIỜ
            if isinstance(dtstart_obj, date) and not isinstance(dtstart_obj, datetime):
                 # Trường hợp sự kiện là CẢ NGÀY (kiểm tra nếu dtstart_obj là đối tượng date nhưng không phải datetime)
                 event_body['start'] = {
                     'date': dtstart_obj.strftime('%Y-%m-%d'), # Định dạng ngày: Michele-MM-DD
                     'timeZone': 'Asia/Ho_Chi_Minh' # Múi giờ chỉ mang tính thông tin cho sự kiện cả ngày trong API
                 }
                 # --- Định dạng thời gian kết thúc (end) cho sự kiện CẢ NGÀY ---
                 if dtend_obj and isinstance(dtend_obj, date) and not isinstance(dtend_obj, datetime):
                     # Nếu DTEND cũng là date object
                     # Google API cần end date cho sự kiện cả ngày là ngày ĐỘC QUYỀN (exclusive), tức là ngày SAU ngày cuối cùng của sự kiện
                     end_date_exclusive = dtend_obj
                     if end_date_exclusive <= dtstart_obj: # Nếu ngày kết thúc trong ICS là cùng ngày hoặc trước ngày bắt đầu trong ICS
                         end_date_exclusive += timedelta(days=1) # Thì ngày kết thúc gửi lên API phải là ngày hôm sau của ngày bắt đầu
                     event_body['end'] = {
                         'date': end_date_exclusive.strftime('%Y-%m-%d'), # Định dạng ngày: Michele-MM-DD
                         'timeZone': 'Asia/Ho_Chi_Minh'
                     }
                 else:
                     # Nếu ICS không có DTEND (hoặc DTEND không phải date), coi sự kiện kéo dài 1 ngày.
                     # Google API cần end date là ngày hôm sau của ngày bắt đầu cho sự kiện kéo dài 1 ngày.
                     event_body['end'] = {
                         'date': (dtstart_obj + timedelta(days=1)).strftime('%Y-%m-%d'), # Ngày bắt đầu + 1 ngày
                         'timeZone': 'Asia/Ho_Chi_Minh'
                     }

            elif isinstance(dtstart_obj, datetime):
                 # Trường hợp sự kiện là CÓ GIỜ CỤ THỂ (kiểm tra nếu dtstart_obj là đối tượng datetime)
                 # Đảm bảo đối tượng datetime có thông tin múi giờ (timezone-aware)
                 if dtstart_obj.tzinfo is None:
                      # Nếu là datetime "ngây thơ" (naive - không có múi giờ), giả định nó ở múi giờ Việt Nam và gán múi giờ
                      dtstart_aware = VIETNAM_TZ.localize(dtstart_obj)
                 else:
                      # Nếu đã có múi giờ, chuyển nó sang múi giờ Việt Nam (hoặc múi giờ mong muốn khác) nếu cần
                      dtstart_aware = dtstart_obj.astimezone(VIETNAM_TZ)

                 # Định dạng thời gian bắt đầu cho API theo chuẩn RFC 3339
                 event_body['start'] = {
                     'dateTime': dtstart_aware.isoformat(), # Định dạng: Michele-MM-DDTHH:MM:SS+HH:MM hoặc Michele-MM-DDTHH:MM:SSZ
                     'timeZone': 'Asia/Ho_Chi_Minh' # Google API đôi khi vẫn cần trường này dù đã có trong isoformat string
                 }

                 # --- Định dạng thời gian kết thúc (end) cho sự kiện CÓ GIỜ ---
                 if dtend_obj and isinstance(dtend_obj, datetime):
                     # Nếu DTEND là datetime
                     if dtend_obj.tzinfo is None:
                         dtend_aware = VIETNAM_TZ.localize(dtend_obj)
                     else:
                         dtend_aware = dtend_obj.astimezone(VIETNAM_TZ)
                     event_body['end'] = {
                         'dateTime': dtend_aware.isoformat(),
                         'timeZone': 'Asia/Ho_Chi_Minh'
                     }
                 else:
                     # Nếu ICS không có DTEND datetime (hoặc DTEND không phải datetime), giả định sự kiện kéo dài 1 giờ.
                     # Chuẩn ICS có thuộc tính DUR (duration) nhưng để đơn giản, ta mặc định 1 giờ nếu thiếu giờ kết thúc.
                     print(f"[{idx}/{total_events}] Cảnh báo: Sự kiện (tiêu đề gốc: '{raw_summary}') có giờ bắt đầu nhưng không có giờ kết thúc. Mặc định kéo dài 1 giờ.")
                     event_body['end'] = {
                         'dateTime': (dtstart_aware + timedelta(hours=1)).isoformat(), # Thời gian bắt đầu + 1 giờ
                         'timeZone': 'Asia/Ho_Chi_Minh'
                     }
            else:
                 # Trường hợp kiểu dữ liệu của dtstart_obj không phải date cũng không phải datetime (lỗi dữ liệu bất thường?)
                 print(f"[{idx}/{total_events}] Bỏ qua sự kiện (tiêu đề gốc: '{raw_summary}') do kiểu dữ liệu DTSTART không rõ.")
                 error_count += 1
                 continue # Chuyển sang sự kiện tiếp theo

            # --- BƯỚC 3: KIỂM SOÁT TỐC ĐỘ (RATE LIMITING) TRƯỚC KHI GỌI API ---
            # Tính thời gian đã trôi qua kể từ lệnh gọi API thành công gần nhất
            current_time = time.time()
            elapsed_since_last_call = current_time - last_call_time
            if elapsed_since_last_call < MIN_DELAY_BETWEEN_CALLS:
                sleep_duration = MIN_DELAY_BETWEEN_CALLS - elapsed_since_last_call
                # print(f"[{idx}/{total_events}] Ngủ {sleep_duration:.4f}s để tuân thủ rate limit.") # Có thể bỏ comment để debug thời gian chờ
                time.sleep(sleep_duration) # Dừng thực thi script trong khoảng thời gian tính toán
            last_call_time = time.time() # Cập nhật thời gian ngay trước khi thực hiện lệnh gọi API tiếp theo

            # --- BƯỚC 4: GỌI API ĐỂ THÊM SỰ KIỆN VÀO GOOGLE CALENDAR ---
            # Thực hiện lệnh gọi API để chèn sự kiện mới vào lịch có ID 'calendar_id'
            # Hàm .execute() sẽ gửi request HTTP thực tế.
            # Thư viện Google API client thường có cơ chế thử lại tự động cho các lỗi tạm thời như 429 (Too Many Requests) hoặc 5xx.
            service.events().insert(calendarId=calendar_id, body=event_body).execute()

            # Nếu lệnh gọi thành công mà không ném exception
            imported_count += 1 # Tăng số đếm sự kiện đã nhập thành công
            # In thông báo tiến trình và kết quả biến đổi tiêu đề/màu (nếu có)
            color_info = f", màu: {event_color_id}" if event_color_id else "" # Chuỗi thông tin về màu nếu có
            print(f"[{imported_count + error_count}/{total_events}] Đã nhập: '{summary}' (gốc: '{raw_summary}'){color_info}")

        except Exception as e:
            # Bắt các loại lỗi có thể xảy ra trong quá trình xử lý hoặc gọi API cho sự kiện hiện tại
            # Điều này giúp script không dừng đột ngột nếu chỉ có một vài sự kiện bị lỗi
            print(f"[{imported_count + error_count}/{total_events}] Lỗi khi xử lý hoặc nhập sự kiện (tiêu đề gốc: '{raw_summary}'): {e}")
            error_count += 1 # Tăng số đếm sự kiện lỗi

        # --- KẾT THÚC XỬ LÝ SỰ KIỆN NÀY ---
        # Vòng lặp tự động chuyển sang component tiếp theo


    # --- BƯỚC 5: TỔNG KẾT QUÁ TRÌNH NHẬP SAU KHI HOÀN THÀNH VÒNG LẶP ---
    print(f"\n--- Tổng kết quá trình nhập lịch ---")
    print(f"Tổng số sự kiện tìm thấy trong tệp ICS: {total_events}")
    print(f"Số sự kiện đã nhập thành công vào Google Calendar: {imported_count}")
    print(f"Số sự kiện gặp lỗi hoặc đã bị bỏ qua trong quá trình xử lý: {error_count}")
    print(f"Quy trình nhập lịch hoàn tất.")


# --- KHỐI THỰC THI CHÍNH CỦA SCRIPT ---
# Đây là điểm bắt đầu khi bạn chạy file Python trực tiếp
if __name__ == '__main__':
    print("--- BẮT ĐẦU QUY TRÌNH NHẬP LỊCH ÂM VÀO GOOGLE CALENDAR ---")

    # Bước 1: Tải và sửa lỗi định dạng file ICS từ URL nguồn
    print("\n--- BƯỚC 1: TẢI VÀ SỬA FILE ICS TỪ URL ---")
    if download_and_fix_ics():
        # Nếu bước 1 thành công, chuyển sang bước 2

        # Bước 2: Xác thực với Google API để có quyền truy cập dịch vụ
        print("\n--- BƯỚC 2: XÁC THỰC GOOGLE API ---")
        google_service = get_service()
        if google_service:
            # Nếu bước 2 thành công (có đối tượng service), chuyển sang bước 3

            # Bước 3: Tìm hoặc tạo lịch Google Calendar đích theo tên
            print("\n--- BƯỚC 3: TÌM HOẶC TẠO LỊCH ĐÍCH ---")
            target_calendar_id = get_or_create_calendar(google_service)
            if target_calendar_id:
                # Nếu bước 3 thành công (có ID lịch), chuyển sang bước 4

                # Bước 4: Nhập các sự kiện từ file ICS đã sửa vào lịch đích (có kiểm soát tốc độ, biến đổi tiêu đề & tô màu)
                print("\n--- BƯỚC 4: NHẬP LỊCH VÀO GOOGLE CALENDAR ---")
                # Gọi hàm nhập lịch, truyền đối tượng service và ID lịch đích
                import_ics_rate_limited(google_service, target_calendar_id)

                # Sau khi hàm nhập kết thúc
                print("\nQuy trình nhập sự kiện hoàn thành.")

            else:
                # Nếu bước 3 lỗi
                print("\nKết thúc quy trình: Không thể tạo hoặc lấy lịch Google Calendar đích. Vui lòng kiểm tra lại quyền truy cập và cấu hình.")
        else:
            # Nếu bước 2 lỗi
            print("\nKết thúc quy trình: Lỗi xác thực Google API. Không thể kết nối tới dịch vụ. Vui lòng kiểm tra lại credentials.json và kết nối mạng.")
    else:
        # Nếu bước 1 lỗi
        print("\nKết thúc quy trình: Không tải xuống và sửa file ICS thành công. Không có dữ liệu nguồn để nhập.")

    print("\n--- KẾT THÚC TOÀN BỘ QUY TRÌNH SCRIPT ---")

