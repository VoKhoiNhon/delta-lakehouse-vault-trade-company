# delta-lakehouse-vault

## I. Basic vault table

### **1. HUB Table**

- **Vai trò**: Lưu trữ **Business Key** độc lập của các thực thể chính (Customer, Product, etc.).
- **Tác dụng**: Là trung tâm của mô hình, kết nối với các bảng Satellite và Link.
- **Trường cơ bản**:
    - **`business_key`**: Business Key duy nhất.
    - **`hub_key`**: Hash key của Business Key.
    - **`load_date`**: Ngày tải dữ liệu.
    - **`record_source`**: Nguồn gốc dữ liệu.
- **Tips**:
    - Hash **`business_key`** để đảm bảo uniqueness.
    - Tránh lưu thông tin mô tả trong HUB.

### **2. SATELLITE Table**

- **Vai trò**: Lưu các thuộc tính chi tiết hoặc lịch sử thay đổi của một thực thể.
- **Tác dụng**: Phân tách dữ liệu biến động khỏi bảng HUB.
- **Trường cơ bản**:
    - **`hash_key`**: Khóa liên kết tới HUB.
    - **`attribute`**: Thuộc tính của thực thể (e.g., tên, địa chỉ).
    - **`valid_from`**, **`valid_to`**: Khoảng thời gian giá trị.
    - **`load_date`**: Ngày tải dữ liệu.
    - **`record_source`**: Nguồn gốc dữ liệu.
    - **`status`**: Trạng thái của đối tượng tại thời điểm đó.
- **Tips**:
    - Tạo nhiều Satellite nếu dữ liệu có tần suất thay đổi khác nhau.

**Mỗi khi trạng thái của đối tượng thay đổi, ta sẽ tạo một hàng mới trong bảng SAT.:**

- Hàng mới này sẽ có cùng khóa ngoại (hash key của hub) với các hàng cũ, nhưng giá trị của cột "status" sẽ được cập nhật thành giá trị mới.
- Cột bổ sung "**`valid_from`**" và "**`valid_to`**" có thể được thêm vào để xác định khoảng thời gian mà trạng thái đó có hiệu lực.
- Cấu trúc bảng SAT:

| hash_key | status   | **`valid_from`** | **`valid_to`** |
| -------- | -------- | ---------------- | -------------- |
| HK_123   | inactive | 2023-01-01       | 2023-03-15     |
| HK_123   | active   | 2023-03-16       | null           |

**Lợi ích của cách làm này:**

- Lịch sử đầy đủ: Ta có thể dễ dàng truy xuất lịch sử thay đổi trạng thái của một đối tượng bất kỳ.
- Tính toàn vẹn: Mỗi thay đổi đều được ghi nhận rõ ràng, đảm bảo tính chính xác của dữ liệu.

---

### **3. LINK Table**

- **Vai trò**: Liên kết các HUB, đại diện cho mối quan hệ giữa các thực thể.
- **Tác dụng**: Lưu thông tin không thuộc về một HUB cụ thể.
- **Trường cơ bản**:
    - **`link_key`**: Hash của các HUB keys liên quan.
    - **`hub1_key`**, **`hub2_key`**, ...: Khóa liên kết các HUB.
    - **`load_date`**: Ngày tải dữ liệu.
    - **`record_source`**: Nguồn gốc dữ liệu.
- **Tips**:
    - Hash composite key của các HUB keys để tạo **`link_key`**.
    - Đừng thêm thông tin chi tiết vào LINK; chúng thuộc về Satellite.
    - **Với data transaction, chỉ cần tạo 1 Link và 1 Satellite, không cần tạo HUB**
        - **Phụ thuộc vào bối cảnh**:
            - **Đúng** nếu giao dịch (transaction) không mang theo một **Business Key** độc lập hoặc không cần truy xuất độc lập. Ví dụ: Một giao dịch có thể liên kết giữa **HUB_CUSTOMER** và **HUB_PRODUCT** qua **LINK_TRANSACTION**.
            - **Sai** nếu giao dịch là một thực thể quan trọng hoặc có **Business Key** riêng (ví dụ: mã giao dịch). Khi đó, cần tạo một **HUB_TRANSACTION** để lưu trữ mã giao dịch.

            **Lưu ý**: Data Vault ưu tiên tính chuẩn hóa, nên nếu giao dịch chỉ kết nối các thực thể chính, việc sử dụng **LINK + SATELLITE** là hợp lý.


---

### **4. Lookup Table**

- **Vai trò**: Lưu các dữ liệu tĩnh, dạng danh mục (categories, codes).
- **Tác dụng**: Giảm lặp thông tin, tăng hiệu quả tra cứu.
- **Trường cơ bản**:
    - **`code`**: Mã danh mục.
    - **`description`**: Mô tả danh mục.
    - **`valid_from`**, **`valid_to`**: Khoảng thời gian hiệu lực.
- **Tips**:
    - Không bắt buộc là thành phần của Data Vault (thường nằm ngoài).

---

### **5. PIT & Bridge Tables (Optional) - business-vault**

- **Vai trò**: Tối ưu hóa truy vấn.
- **Tác dụng**:
    - **PIT (Point-in-Time Table)**: Tăng tốc truy vấn dữ liệu tại thời điểm cụ thể.
    - **Bridge Table**: Tạo các tập dữ liệu tổng hợp.
- **Tips**:
    - Dùng khi hiệu suất truy vấn trên các bảng HUB/SAT/LINK không đáp ứng đủ.
