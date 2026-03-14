/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  async rewrites() {
    const internalAPIBase = (process.env.INTERNAL_API_URL || 'http://ota-api:8080').replace(/\/$/, '')
    return [
      {
        source: '/api/:path*',
        destination: `${internalAPIBase}/api/:path*`,
      },
    ]
  },
}
module.exports = nextConfig
