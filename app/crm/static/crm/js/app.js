import { initAnimatedDisclosures } from "./core/animatedDisclosure.js";
import { initColumnPickerForms } from "./core/columnPicker.js";
import { initDevLiveReload } from "./core/devLiveReload.js";
import { initDropdownMenus } from "./core/dropdownMenu.js";
import { initTableActions } from "./core/tableActions.js";
import { initDynamicFormsets } from "./forms/dynamicFormset.js";
import { initEnhancedSelects } from "./forms/enhancedSelect.js";
import { initLiveRelationshipPickers } from "./forms/relationshipPicker.js";
import { initSingleAutocompletes } from "./forms/singleAutocomplete.js";

initDropdownMenus();
initColumnPickerForms();
initAnimatedDisclosures();
initTableActions();
initEnhancedSelects();
initLiveRelationshipPickers();
initSingleAutocompletes();
initDynamicFormsets();
initDevLiveReload();
