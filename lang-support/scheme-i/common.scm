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

(require srfi/1)  ; list library
(require srfi/13) ; string library

;;; definitions common to both mapper and reducer wrappers

;;; internal library functions

(define _wmr_split_ 
  ;; 1 arg, any string
  ;; return a list (key value) where key is characters of arg1 before 
  ;;   first tab, value is characters after first tab.
  ;;   If no tab in arg1, then use key=arg1 value=""
  (lambda (str) ; any string
    (let ((pos (string-index str #\tab)))
      (if pos
       (list (substring str 0 pos) (substring str (+ pos 1)))
       (list str "")))))

;;; public library functions

(define wmr-emit
  (case-lambda
   ((key val) (wmr-emit key val display))
   ((key val output-func)
  	(if (and (null? key) (null? val)) #f
        (begin (output-func key) (display "\t") (output-func val) (display "\n"))))))

(define wmr-split
  (case-lambda
    ; string to split
    ((str) (wmr-split str " +"))
    ; string to split, delimiter regex
    ((str delim)
     (regexp-split delim str))
    ; string to split, delimiter regx, maximum # of tokens
    ((str delim maxct)
     (let ((tokens (regexp-split delim str)))
      (if (>= maxct (length tokens)) tokens
          (append (take tokens (- maxct 1))
                  (list (string-join (drop tokens (- maxct 1))))))))))

