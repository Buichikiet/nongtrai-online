/* Chỉ cần sửa file này khi đặt giao diện (public/) ở web tĩnh khác với server.
   Khi chạy bằng "npm start", server tự trả /config.js nên không cần sửa. */
window.CFG = {
  googleClientId: "",          // Google OAuth Client ID
  server: "",                  // vd: "https://nongtrai-server.onrender.com" (để trống = cùng domain)
  dev: false
};
