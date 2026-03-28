import { initAnimatedDisclosures } from "./core/animatedDisclosure.js";
import { initColumnPickerForms } from "./core/columnPicker.js";
import { initDevLiveReload } from "./core/devLiveReload.js";
import { initDynamicFormsets } from "./forms/dynamicFormset.js";
import { initLiveRelationshipPickers } from "./forms/relationshipPicker.js";
import { initSingleAutocompletes } from "./forms/singleAutocomplete.js";

initColumnPickerForms();
initAnimatedDisclosures();
initLiveRelationshipPickers();
initSingleAutocompletes();
initDynamicFormsets();
initDevLiveReload();
