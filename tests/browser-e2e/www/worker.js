import init, { start } from './pkg/browser_e2e.js';

onmessage = async (e) => {
  try {
    await init();
    start(e.data.ws, e.data.channel);
    postMessage('started');
  } catch (err) {
    postMessage(String(err));
  }
};
