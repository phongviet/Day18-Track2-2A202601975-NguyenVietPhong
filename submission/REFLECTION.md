# Reflection: Top 5 Lakehouse Anti-Patterns

### Anti-Pattern được chọn: Stale External Vector Index & Lifecycle Decoupling (Lệch pha vòng đời Vector ngoài)

Trong các hệ sinh thái AI & LLM hiện nay, dự án của chúng tôi có nguy cơ cao nhất đối với anti-pattern **Stale External Vector Index (Vector Index tách rời bị lỗi thời)**.

Khi người dùng yêu cầu xoá dữ liệu cá nhân (tuân thủ Nghị định 13 / GDPR) hoặc tài liệu nội bộ bị thu hồi, bảng Delta Lake / Iceberg trong Lakehouse xử lý xoá và tombstone bản ghi rất chuẩn xác theo giao dịch ACID. Tuy nhiên, các Vector DB chuyên dụng bên ngoài (như Pinecone, Qdrant) thường chỉ được cập nhật qua các pipeline đồng bộ bất đồng bộ hoặc cron job có độ trễ.

Như đã đo đạc và chứng minh trực tiếp trong Lab 18 (NB7), khi tài liệu đã biến mất hoàn toàn khỏi bảng dữ liệu gốc nhưng index vector ngoài chưa kịp xoá, hệ thống RAG và AI Agent vẫn tiếp tục truy vấn trúng embedding cũ, gây ảo giác (hallucination) và rò rỉ dữ liệu nghiêm trọng. Việc chuyển đổi sang mô hình **Lakehouse-native Vector** (lưu vector và metadata trực tiếp trong bảng Parquet/Delta) là giải pháp triệt để loại bỏ rủi ro lệch pha này.
