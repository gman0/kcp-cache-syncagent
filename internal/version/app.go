/*
Copyright 2026 The KCP Authors.

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

package version

// These variables get fed by ldflags during compilation.
var (
	// gitVersion is a variable containing the git commit identifier
	// (usually the output of `git describe`, i.e. not necessarily a
	// static tag name); for a tagged Sync Agent release, this value is identical
	// to kubermaticDockerTag, for untagged builds this is the `git describe`
	// output.
	// Importantly, this value will only ever go up, even for untagged builds,
	// but is not monotone (gaps can occur, this can go from v2.20.0-1234-d6aef3
	// to v2.34.0-912-dd79178e to v3.0.1).
	// Also this value does not necessarily reflect the current release branch,
	// as releases are tagged on the release branch and on those tags are not
	// visible from the main branch.
	gitVersion string
	// gitHead is the full SHA hash of the Git commit the application was built for.
	gitHead string
)

type AppVersion struct {
	GitVersion string
	GitHead    string
}

func NewAppVersion() AppVersion {
	return AppVersion{
		GitVersion: gitVersion,
		GitHead:    gitHead,
	}
}

func NewFakeAppVersion() AppVersion {
	return AppVersion{
		GitVersion: "v0.0.0-42-test",
		GitHead:    "d9c09114135c62e207b30891899e7e1ad2493f38",
	}
}
