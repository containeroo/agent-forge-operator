/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
)

func recordEvent(recorder events.EventRecorder, object runtime.Object, eventtype, reason, message string) {
	recorder.Eventf(object, nil, eventtype, reason, reason, "%s", message)
}

func recordEventf(recorder events.EventRecorder, object runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	recorder.Eventf(object, nil, eventtype, reason, reason, messageFmt, args...)
}
