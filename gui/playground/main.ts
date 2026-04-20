import { flick } from '@ratiosolver/flick';
import { coco } from '../src/coco';
import { CoCoApp } from '../src/components/app';
import '@fortawesome/fontawesome-free/css/all.css';

const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
const cc = new coco.CoCo({ url: wsUrl });

flick.mount(() => CoCoApp(cc));

cc.connect();