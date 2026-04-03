import { flick } from '@ratiosolver/flick';
import { coco } from '../src/coco';
import { CoCoApp } from '../src/components/app';
import '@fortawesome/fontawesome-free/css/all.css';

const cc = new coco.CoCo({ url: 'ws://localhost:3000/ws' });

flick.mount(() => CoCoApp(cc));

cc.connect();