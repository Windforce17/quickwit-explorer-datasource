import CopyWebpackPlugin from 'copy-webpack-plugin';
import ForkTsCheckerWebpackPlugin from 'fork-ts-checker-webpack-plugin';
import path from 'path';
import { fileURLToPath } from 'url';
import type { Configuration } from 'webpack';

// @ts-ignore
import ReplaceInFileWebpackPlugin from 'replace-in-file-webpack-plugin';

const __filename2 = fileURLToPath(import.meta.url);
const __dirname2 = path.dirname(__filename2);

const config = (env: Record<string, string>): Configuration => ({
  cache: { type: 'filesystem' },
  context: path.join(__dirname2, 'src'),
  devtool: env.production ? 'source-map' : 'eval-source-map',
  entry: './module.ts',
  externals: [
    'lodash',
    'react',
    'react-dom',
    '@grafana/data',
    '@grafana/runtime',
    '@grafana/ui',
    function (data: any, callback: any) {
      const request = data.request;
      const prefix = 'grafana/';
      if (request && request.indexOf(prefix) === 0) {
        return callback(null, request.slice(prefix.length));
      }
      callback();
    },
  ] as any,
  mode: env.production ? 'production' : 'development',
  module: {
    rules: [
      {
        exclude: /(node_modules)/,
        test: /\.[tj]sx?$/,
        use: {
          loader: 'swc-loader',
        },
      },
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.(png|jpe?g|gif|svg)$/,
        type: 'asset/resource',
        generator: {
          filename: 'img/[hash][ext]',
          publicPath: 'public/plugins/quickwit-explorer-datasource/',
        },
      },
    ],
  },
  output: {
    clean: true,
    filename: 'module.js',
    library: { type: 'amd' },
    path: path.resolve(__dirname2, 'dist'),
    publicPath: '/',
  },
  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        { from: 'plugin.json', to: '.' },
        { from: '../README.md', to: '.', noErrorOnMissing: true },
        { from: 'img/', to: 'img/', noErrorOnMissing: true },
      ],
    }),
    new ForkTsCheckerWebpackPlugin({
      async: Boolean(env.development),
      issue: {
        include: [{ file: 'src/**/*.{ts,tsx}' }],
      },
      typescript: { configFile: path.join(__dirname2, 'tsconfig.json') },
    }),
    ...(env.production
      ? [
          new ReplaceInFileWebpackPlugin([
            {
              dir: 'dist',
              files: ['plugin.json'],
              rules: [
                {
                  search: '%VERSION%',
                  replace: '1.0.0',
                },
                {
                  search: '%TODAY%',
                  replace: new Date().toISOString().substring(0, 10),
                },
              ],
            },
          ]),
        ]
      : []),
  ],
  resolve: {
    extensions: ['.js', '.jsx', '.ts', '.tsx'],
  },
});

export default config;
