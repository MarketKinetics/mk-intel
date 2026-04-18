/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        navy: {
          900: '#0A1628',
          800: '#0D1F3C',
          700: '#102847',
        },
        teal: {
          accent: '#14C9B8',
          dark: '#0D7377',
        },
        ink: '#0F1923',
        slate: '#5C6B7A',
        surface: '#F8F7F4',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
