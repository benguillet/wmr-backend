; Copyright 2010 WebMapReduce Developers
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;	http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(include "common.scm")

;;; definitions specific to mapper wrappers

(define _wmr_read_pair_
  (lambda ()
    (let ((line (read-line)))
     (if (eof-object? line) line
         (_wmr_split_ line)))))

(define _wmr_apply_mapper_
  (lambda (mapper pair)
   (if (eof-object? pair) #t
       (begin (for-each _wmr_emit_pair_ (mapper (car pair) (cadr pair)))
              (_wmr_apply_mapper_ mapper (_wmr_read_pair_))))))

