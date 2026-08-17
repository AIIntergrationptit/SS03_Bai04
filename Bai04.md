1. Sơ đồ luồng dữ liệu ETL (ASCII Architecture Diagram)Plaintext┌────────────────┐
│  CV Thô (Raw)  │ (Văn bản phi cấu trúc từ ứng viên)
└───────┬────────┘
        │
        ▼ [EXTRACT & TRANSFORM]
┌────────────────────────────────────────────────────────┐
│  Spring AI ChatModel + BeanOutputConverter             │
│  - Tạo Prompt kèm Schema định dạng                     │
│  - Gửi Request tới LLM (Ollama/OpenRouter)             │
│  - Jackson parse JSON ──► Java Record                  │
└───────┬────────────────────────────────────────────────┘
        │
        ▼ (CandidateExtraction Record)
┌────────────────────────────────────────────────────────┐
│  Business Validation Layer                             │
│  - Validate 1: Họ tên & Email không trống, đúng Regex  │
│  - Validate 2: Số năm kinh nghiệm >= 0                 │
└───────┬────────────────────────────────────────────────┘
        │
        ▼ (Dữ liệu hợp lệ)
┌────────────────────────────────────────────────────────┐
│  [LOAD] CandidateRepository.save(candidateEntity)      │
│  - Mở Transaction                                      │
│  - Insert vào Database (PostgreSQL/MySQL)              │
│  - Commit Transaction & Trả kết quả ID                 │
└───────┬────────────────────────────────────────────────┘
        │
        ▼
┌────────────────┐
│ Database Table │ (Dữ liệu ứng viên đã chuẩn hóa)
└────────────────┘
2. Phân tích Trade-off: Đặt LLM Call trong vs. ngoài phạm vi @TransactionalTiêu chí phân tíchĐặt LLM Call BÊN TRONG @TransactionalĐặt LLM Call BÊN NGOÀI @Transactional (Khuyên dùng)Chiếm dụng kết nối CSDL (Connection Pooling)Cực kỳ nguy hiểm: Khi bước vào method có @Transactional, Spring lấy ngay một DB Connection từ HikariCP Pool. LLM phản hồi mất 5–20 giây, làm connection bị khóa nhàn rỗi suốt thời gian chờ mạng $\rightarrow$ Dẫn đến cạn kiệt Connection Pool (Pool starvation), sập toàn bộ các luồng web khác.Tối ưu: Connection chỉ được lấy ra từ Pool khi LLM đã xử lý xong và dữ liệu đã sẵn sàng để lưu vào DB (chỉ tốn vài mili-giây cho thao tác INSERT).Cơ chế Rollback khi có sự cốRollback tự động toàn diện nếu LLM văng lỗi (nhưng không mang lại nhiều giá trị vì chưa có dữ liệu nào được ghi xuống DB trước khi gọi LLM).Dễ dàng xử lý tách biệt: Nếu LLM lỗi thì ngắt ngay lập tức mà không ảnh hưởng tới trạng thái DB; Transaction chỉ quản lý duy nhất giai đoạn lưu dữ liệu.Tính chịu tải hệ thống (Throughput & Scalability)Kém, hệ thống nhanh chóng bị nghẽn (bottleneck) khi có nhiều request đồng thời.Rất cao, tận dụng hiệu quả tài nguyên server và khả năng chịu tải của CSDL.3. Mã nguồn Java hoàn chỉnhA. Record CandidateExtraction.javaJavapackage com.rikkeiacademy.dto;

import java.util.List;

public record CandidateExtraction(
        String fullName,
        String phone,
        String email,
        List<String> skills,
        int yearsExperience
) {}
B. JPA Entity Candidate.javaJavapackage com.rikkeiacademy.entity;

import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "candidates")
public class Candidate {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String fullName;

    private String phone;

    @Column(nullable = false)
    private String email;

    @ElementCollection
    @CollectionTable(name = "candidate_skills", joinColumns = @JoinColumn(name = "candidate_id"))
    @Column(name = "skill")
    private List<String> skills;

    @Column(name = "years_experience")
    private int yearsExperience;

    public Candidate() {}

    public Candidate(String fullName, String phone, String email, List<String> skills, int yearsExperience) {
        this.fullName = fullName;
        this.phone = phone;
        this.email = email;
        this.skills = skills;
        this.yearsExperience = yearsExperience;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public String getFullName() { return fullName; }
    public String getPhone() { return phone; }
    public String getEmail() { return email; }
    public List<String> getSkills() { return skills; }
    public int getYearsExperience() { return yearsExperience; }
}
C. Repository CandidateRepository.javaJavapackage com.rikkeiacademy.repository;

import com.rikkeiacademy.entity.Candidate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CandidateRepository extends JpaRepository<Candidate, Long> {
}
D. Service CandidateETLService.javaJavapackage com.rikkeiacademy.service;

import com.rikkeiacademy.dto.CandidateExtraction;
import com.rikkeiacademy.entity.Candidate;
import com.rikkeiacademy.repository.CandidateRepository;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.regex.Pattern;

@Service
public class CandidateETLService {

    private final ChatModel chatModel;
    private final CandidateRepository candidateRepository;
    private static final Pattern EMAIL_REGEX = Pattern.compile("^[A-Za-z0-9+_.-]+@(.+)$");

    public CandidateETLService(ChatModel chatModel, CandidateRepository candidateRepository) {
        this.chatModel = chatModel;
        this.candidateRepository = candidateRepository;
    }

    /**
     * Phương thức chính thực hiện luồng ETL:
     * Lưu ý: Không đặt @Transactional ở cấp toàn bộ method để tránh chiếm dụng Connection Pool khi gọi LLM.
     */
    public Long processResume(String resumeText) {
        // BƯỚC 1: EXTRACT & TRANSFORM (GỌI LLM BÊN NGOÀI TRANSACTION)
        CandidateExtraction extraction = extractCandidateInfo(resumeText);

        // BƯỚC 2: VALIDATE NGHIỆP VỤ
        validateCandidateData(extraction);

        // BƯỚC 3: LOAD (LƯU VÀO DATABASE VỚI TRANSACTION CỤC BỘ)
        return saveCandidateData(extraction);
    }

    private CandidateExtraction extractCandidateInfo(String resumeText) {
        BeanOutputConverter<CandidateExtraction> converter = new BeanOutputConverter<>(CandidateExtraction.class);

        String template = """
                [VAI TRÒ] Bạn là chuyên gia trích xuất thông tin hồ sơ ứng viên (HR Resume Parser).
                [NHIỆM VỤ] Đọc văn bản CV thô sau và bóc tách chính xác các trường dữ liệu theo định dạng schema yêu cầu.
                
                [RÀNG BUỘC]
                1. Chỉ trả về chuỗi JSON thuần, bắt đầu bằng '{' và kết thúc bằng '}'.
                2. Tuyệt đối không bọc trong markdown code fence (```json).
                3. Không thêm bất kỳ lời dẫn hay bình luận nào.
                
                [NỘI DUNG CV THÔ]:
                \"\"\"
                {resumeText}
                \"\"\"
                
                [ĐỊNH DẠNG ĐẦU RA]:
                {formatInstructions}
                """;

        Prompt prompt = new PromptTemplate(template).create(Map.of(
                "resumeText", resumeText,
                "formatInstructions", converter.getFormatInstructions()
        ));

        ChatResponse response = chatModel.call(prompt);
        String jsonText = response.getResult().getOutput().getText();

        try {
            return converter.convert(jsonText);
        } catch (Exception e) {
            throw new IllegalArgumentException("Không thể parse dữ liệu từ CV: " + e.getMessage());
        }
    }

    private void validateCandidateData(CandidateExtraction data) {
        // Validation 1: Họ tên và Email không được trống, Email đúng regex
        if (data.fullName() == null || data.fullName().trim().isEmpty()) {
            throw new IllegalArgumentException("Validation Error: Họ tên ứng viên không được để trống.");
        }
        if (data.email() == null || !EMAIL_REGEX.matcher(data.email().trim()).matches()) {
            throw new IllegalArgumentException("Validation Error: Email ứng viên không hợp lệ (" + data.email() + ").");
        }

        // Validation 2: Số năm kinh nghiệm phải >= 0
        if (data.yearsExperience() < 0) {
            throw new IllegalArgumentException("Validation Error: Số năm kinh nghiệm không thể âm (" + data.yearsExperience() + ").");
        }
    }

    @Transactional
    public Long saveCandidateData(CandidateExtraction data) {
        Candidate entity = new Candidate(
                data.fullName().trim(),
                data.phone() != null ? data.phone().trim() : null,
                data.email().trim(),
                data.skills(),
                data.yearsExperience()
        );
        return candidateRepository.save(entity).getId();
    }
}
4. Minh chứng chạy thực tế (Text Log)Prompt gửi tới LLM:Plaintext[VAI TRÒ] Bạn là chuyên gia trích xuất thông tin hồ sơ ứng viên (HR Resume Parser).
[NHIỆM VỤ] Đọc văn bản CV thô sau và bóc tách chính xác các trường dữ liệu theo định dạng schema yêu cầu.

[RÀNG BUỘC]
1. Chỉ trả về chuỗi JSON thuần, bắt đầu bằng '{' và kết thúc bằng '}'.
2. Tuyệt đối không bọc trong markdown code fence (```json).
3. Không thêm bất kỳ lời dẫn hay bình luận nào.

[NỘI DUNG CV THÔ]:
"""
Ứng viên: Lê Hoàng Long
Email liên hệ: hoanglong.le@gmail.com - SĐT: 0987654321
Kinh nghiệm làm việc: Có 4 năm kinh nghiệm lập trình Backend với Java Spring Boot, Microservices, Docker, PostgreSQL và Redis.
"""

[ĐỊNH DẠNG ĐẦU RA]:
The output should be formatted as a JSON instance that conforms to the JSON schema below.
{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"fullName":{"type":"string"},"phone":{"type":"string"},"email":{"type":"string"},"skills":{"type":"array","items":{"type":"string"}},"yearsExperience":{"type":"integer"}},"required":["fullName","phone","email","skills","yearsExperience"]}
Kết quả JSON sạch trả về từ LLM:JSON{
  "fullName": "Lê Hoàng Long",
  "phone": "0987654321",
  "email": "hoanglong.le@gmail.com",
  "skills": ["Java Spring Boot", "Microservices", "Docker", "PostgreSQL", "Redis"],
  "yearsExperience": 4
}