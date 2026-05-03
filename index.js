// index.js — Payment + Notification Service (port 8084)
require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { connect, publish, consume } = require("./rabbitmq");

const app = express();
const ORDER_API = process.env.ORDER_API || "http://172.29.48.1:8083";
const PORT = process.env.PORT || 8084;
const SUCCESS_RATE = parseFloat(process.env.SUCCESS_RATE || "0.8");

app.use(cors());
app.use(express.json());

/* ─── Cập nhật trạng thái booking ────────────────────────── */
const updateBookingStatus = async (bookingId, status) => {
  try {
    const res = await fetch(`${ORDER_API}/bookings/${bookingId}/status`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    });
    if (res.ok) {
      console.log(`[Booking] ✅ Booking #${bookingId} → ${status}`);
      return await res.json();
    }
    console.warn(`[Booking] ⚠️ Không cập nhật được: ${res.status}`);
  } catch (err) {
    console.warn(`[Booking] ⚠️ Lỗi:`, err.message);
  }
  return null;
};

/* ─── POST /payments — Frontend gọi vào ──────────────────── */
app.post("/payments", async (req, res) => {
  const { bookingId } = req.body;

  if (!bookingId) {
    return res.status(400).json({ error: "bookingId là bắt buộc." });
  }

  console.log(`\n[Payment] 📥 Yêu cầu thanh toán — Booking #${bookingId}`);

  // Giả lập xử lý thanh toán
  await new Promise((r) => setTimeout(r, 800));

  const isSuccess = Math.random() < SUCCESS_RATE;

  if (isSuccess) {
    console.log(`[Payment] ✅ Thành công — Booking #${bookingId}`);

    const updated = await updateBookingStatus(bookingId, "SUCCESS");

    // Publish vào queue PAYMENT_COMPLETED — Notification sẽ consume
    await publish("PAYMENT_COMPLETED", {
      bookingId,
      userId: updated?.userId,
      movieId: updated?.movieId,
      seats: updated?.seats,
      showtime: updated?.showtime,
      totalPrice: updated?.totalPrice,
      paidAt: new Date(),
    });

    return res.json({
      status: "SUCCESS",
      bookingId,
      message: "Thanh toán thành công!",
    });
  } else {
    console.log(`[Payment] ❌ Thất bại — Booking #${bookingId}`);

    await updateBookingStatus(bookingId, "FAILED");

    await publish("BOOKING_FAILED", {
      bookingId,
      reason: "Thanh toán thất bại (ngẫu nhiên)",
      failedAt: new Date(),
    });

    return res.json({
      status: "FAILED",
      bookingId,
      message: "Thanh toán thất bại. Vui lòng thử lại.",
    });
  }
});

/* ─── Notification: consume PAYMENT_COMPLETED ────────────── */
const startNotification = async () => {
  await consume("PAYMENT_COMPLETED", (data) => {
    const amount = data.totalPrice
      ? new Intl.NumberFormat("vi-VN").format(data.totalPrice) + " ₫"
      : "N/A";

    console.log("\n╔══════════════════════════════════════════════════╗");
    console.log("║              🎬 THÔNG BÁO ĐẶT VÉ                ║");
    console.log("╠══════════════════════════════════════════════════╣");
    console.log(
      `║  Booking #${String(data.bookingId).slice(-6).toUpperCase().padEnd(39)}║`,
    );
    console.log(`║  Phim:    ${String(data.movieId ?? "N/A").padEnd(40)}║`);
    console.log(`║  Ghế:     ${String(data.seats ?? 1).padEnd(40)}║`);
    console.log(`║  Suất:    ${String(data.showtime ?? "N/A").padEnd(40)}║`);
    console.log(`║  Tổng:    ${amount.padEnd(40)}║`);
    console.log("╠══════════════════════════════════════════════════╣");
    console.log("║  ✅ Đặt vé thành công!                           ║");
    console.log("╚══════════════════════════════════════════════════╝\n");
  });

  await consume("BOOKING_FAILED", (data) => {
    console.log("\n╔══════════════════════════════════════════════════╗");
    console.log("║              ❌ THÔNG BÁO THẤT BẠI               ║");
    console.log("╠══════════════════════════════════════════════════╣");
    console.log(
      `║  Booking #${String(data.bookingId).slice(-6).toUpperCase().padEnd(39)}║`,
    );
    console.log(
      `║  Lý do:   ${String(data.reason ?? "Không xác định").padEnd(40)}║`,
    );
    console.log("╚══════════════════════════════════════════════════╝\n");
  });
};

/* ─── Start ───────────────────────────────────────────────── */
const start = async () => {
  console.log("🚀 Payment + Notification Service đang khởi động...");

  await connect(); // kết nối RabbitMQ 1 lần duy nhất

  await startNotification(); // đăng ký consumers

  app.listen(PORT, () => {
    console.log(`✅ Payment Service chạy tại http://172.29.48.1:${PORT}`);
    console.log(`   POST /payments → xử lý thanh toán\n`);
  });
};

start().catch((err) => {
  console.error("❌ Lỗi khởi động:", err.message);
  process.exit(1);
});
