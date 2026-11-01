// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemaregistry

import "fmt"

type Error struct {
	Status  int
	Code    int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

func newError(status, code int, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

func errSubjectNotFound(subject string) *Error {
	return newError(404, 40401, "Subject '%s' not found.", subject)
}

func errVersionNotFound(version int) *Error {
	return newError(404, 40402, "Version %d not found.", version)
}

func errSchemaNotFound() *Error {
	return newError(404, 40403, "Schema not found")
}

func errInvalidSchema(format string, args ...any) *Error {
	return newError(422, 42201, "Invalid schema: "+format, args...)
}

func errInvalidVersion(version string) *Error {
	return newError(422, 42202, "The specified version '%s' is not a valid version id. Allowed values are between [1, 2^31-1] and the string \"latest\"", version)
}

func errInvalidCompatibility(level string) *Error {
	return newError(422, 42203, "Invalid compatibility level %q. Supported levels are NONE and BACKWARD.", level)
}

func errInvalidMode(mode string) *Error {
	return newError(422, 42204, "Invalid mode %q. Valid values are READWRITE, READONLY and IMPORT.", mode)
}

func errNotPermitted(format string, args ...any) *Error {
	return newError(422, 42205, format, args...)
}

func errReferenceExists(subject string, version int) *Error {
	return newError(422, 42206, "One or more references exist to the schema {subject=%s,version=%d}.", subject, version)
}

func errIncompatible(reason string) *Error {
	return newError(409, 409, "Schema being registered is incompatible with an earlier schema for subject: %s", reason)
}

func errSubjectSoftDeleted(subject string) *Error {
	return newError(404, 40404, "Subject '%s' was soft deleted. Set permanent=true to delete permanently", subject)
}

func errSubjectNotSoftDeleted(subject string) *Error {
	return newError(404, 40405, "Subject '%s' was not deleted first before being permanently deleted", subject)
}

func errVersionSoftDeleted(subject string, version int) *Error {
	return newError(404, 40406, "Subject '%s' Version %d was soft deleted. Set permanent=true to delete permanently", subject, version)
}

func errVersionNotSoftDeleted(subject string, version int) *Error {
	return newError(404, 40407, "Subject '%s' Version %d was not deleted first before being permanently deleted", subject, version)
}

func errSubjectConfigNotFound(subject string) *Error {
	return newError(404, 40408, "Subject '%s' does not have subject-level compatibility configured", subject)
}

func errSubjectModeNotFound(subject string) *Error {
	return newError(404, 40409, "Subject '%s' does not have subject-level mode configured", subject)
}

func errBadRequest(format string, args ...any) *Error {
	return newError(422, 422, format, args...)
}

func errStore(err error) *Error {
	return newError(500, 50001, "Error in the backend data store: %v", err)
}

func errTimeout(err error) *Error {
	return newError(500, 50002, "Operation timed out: %v", err)
}
