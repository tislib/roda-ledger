import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg: {
          0: '#0a0a0b',
          1: '#111114',
          2: '#17171c',
          3: '#1d1d24',
        },
        border: {
          subtle: '#1f1f26',
          DEFAULT: '#2a2a33',
          strong: '#3a3a47',
        },
        text: {
          primary: '#e8e8ec',
          secondary: '#a8a8b3',
          muted: '#6b6b78',
        },
        accent: {
          DEFAULT: '#f5a524',
          dim: '#8a5d14',
        },
        role: {
          leader: '#f5a524',
          follower: '#5b8def',
          candidate: '#c084fc',
          init: '#6b6b78',
        },
        health: {
          up: '#3ecf8e',
          paused: '#f5a524',
          isolated: '#c084fc',
          crashed: '#ef4444',
        },
      },
      fontFamily: {
        mono: [
          'JetBrains Mono',
          'Berkeley Mono',
          'ui-monospace',
          'SFMono-Regular',
          'Menlo',
          'Consolas',
          'monospace',
        ],
        sans: [
          'Inter',
          'ui-sans-serif',
          'system-ui',
          '-apple-system',
          'BlinkMacSystemFont',
          'Segoe UI',
          'sans-serif',
        ],
      },
      transitionTimingFunction: {
        'linear-fast': 'cubic-bezier(0.4, 0, 0.2, 1)',
      },
      keyframes: {
        flash: {
          '0%, 100%': { backgroundColor: 'transparent' },
          '50%': { backgroundColor: 'rgba(245, 165, 36, 0.25)' },
        },
        pulse: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.5' },
        },
      },
      animation: {
        flash: 'flash 800ms ease-out',
        'pulse-slow': 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
    },
  },
  plugins: [],
} satisfies Config;
