C**** DBGEMIT
c
c-----------------------------------------------------------------------
c
c   Debug intermediate-state emit subroutines for moves.rs Phase 0.
c
c   When the environment variable NRDBG_FILE is set, calls to the
c   dbg* subroutines append tagged records to that file. When unset,
c   they are no-ops (cheap: a single saved-flag check + return).
c
c   The records produced here become the regression baseline for the
c   Rust NONROAD port (Phase 5). The format is line-oriented TSV so
c   it can be loaded into Polars/pandas without a parser:
c
c     <phase>\t<context>\t<label>\t<count>\t<v1>\t<v2>...\n
c
c   <phase>   subsystem label, one of {GETPOP, AGEDIST, GRWFAC, CLCEMS}
c   <context> caller-provided key=val,key=val tag string identifying
c             which call this is (FIPS, SCC, year, equipment idx, ...)
c   <label>   variable-name tag for the array/scalar
c   <count>   number of values that follow
c   <v1>...   space-or-tab separated values
c
c   Long arrays are emitted on a single physical line; the line buffer
c   is sized to MXAGYR*32 chars + overhead, which fits the longest
c   per-call array in NONROAD (mdyrfrc / emsbmy have MXAGYR=51 entries).
c   Larger COMMON arrays (popeqp, etc.) are emitted by the patched
c   getpop.f one record per equipment row, not as a single mega-array.
c
c-----------------------------------------------------------------------
c   LOG:
c-----------------------------------------------------------------------
c
c   2026-05-07  --moves.rs--  original development for mo-abxb
c
c-----------------------------------------------------------------------
c   Subroutines provided:
c
c     dbgini()                       initialize / open output file
c     dbgr1(phase, ctx, label, n, arr)   write real*4 1D array
c     dbgi1(phase, ctx, label, n, arr)   write integer*4 1D array
c     dbgrs(phase, ctx, label, val)      write real*4 scalar
c     dbgis(phase, ctx, label, val)      write integer*4 scalar
c     dbgon(flag)                    return .TRUE. iff debug active
c
c-----------------------------------------------------------------------

      subroutine dbgini()
c
c     Open the debug output file the first time we are called. The
c     file path comes from environment variable NRDBG_FILE. If that
c     env var is unset or empty, dbgini sets a sticky disabled-flag
c     and all subsequent dbg* calls are no-ops.
c
      implicit none
c
      logical*4   ldone, lopen
      common /dbgcom/ ldone, lopen
      save   /dbgcom/
c
      character*1024 path
      integer*4      ios
c
c   --- already initialized? short-circuit ---
c
      if( ldone ) goto 9999
      ldone = .TRUE.
      lopen = .FALSE.
c
c   --- read env var; gfortran's getenv writes spaces if unset ---
c
      call getenv('NRDBG_FILE', path)
      if( path .EQ. ' ' ) goto 9999
c
c   --- open for append ---
c
      open(unit=98, file=path, status='unknown', position='append',
     &     iostat=ios)
      if( ios .NE. 0 ) goto 9999
      lopen = .TRUE.
c
 9999 continue
      return
      end


      subroutine dbgon(flag)
c
c     Predicate: is debug emission active for this run?
c
      implicit none
      logical*4   flag
c
      logical*4   ldone, lopen
      common /dbgcom/ ldone, lopen
      save   /dbgcom/
c
      if( .NOT. ldone ) call dbgini()
      flag = lopen
      return
      end


      subroutine dbgr1(phase, ctx, label, n, arr)
c
c     Emit a real*4 1D array as a single TSV record. Uses
c     non-advancing writes so the line length is bounded only by the
c     gfortran record-length limit (effectively unlimited at runtime),
c     not by a format-descriptor repeat count. Earlier drafts used a
c     `1024(a1,1pe14.7)` repeat which would silently start a new
c     record after the 1024th value — would break popeqp(MXPOP=1000)
c     today and any future MXPOP bump.
c
      implicit none
      character*(*) phase, ctx, label
      integer*4     n
      real*4        arr(*)
c
      logical*4   ldone, lopen
      common /dbgcom/ ldone, lopen
      save   /dbgcom/
c
      integer*4   i, lp, lc, ll
      integer*4   strmin
      external    strmin
c
      if( .NOT. ldone ) call dbgini()
      if( .NOT. lopen ) return
c
      lp = strmin(phase)
      lc = strmin(ctx)
      ll = strmin(label)
c
      write(98,'(a,a1,a,a1,a,a1,i0)',advance='no')
     &      phase(1:lp), char(9), ctx(1:lc), char(9), label(1:ll),
     &      char(9), n
      do i = 1, n
          write(98,'(a1,1pe14.7)',advance='no') char(9), arr(i)
      end do
      write(98,'(a)') ''
      return
      end


      subroutine dbgi1(phase, ctx, label, n, arr)
c
c     Emit an integer*4 1D array as a single TSV record. See dbgr1
c     for the non-advancing rationale.
c
      implicit none
      character*(*) phase, ctx, label
      integer*4     n
      integer*4     arr(*)
c
      logical*4   ldone, lopen
      common /dbgcom/ ldone, lopen
      save   /dbgcom/
c
      integer*4   i, lp, lc, ll
      integer*4   strmin
      external    strmin
c
      if( .NOT. ldone ) call dbgini()
      if( .NOT. lopen ) return
c
      lp = strmin(phase)
      lc = strmin(ctx)
      ll = strmin(label)
c
      write(98,'(a,a1,a,a1,a,a1,i0)',advance='no')
     &      phase(1:lp), char(9), ctx(1:lc), char(9), label(1:ll),
     &      char(9), n
      do i = 1, n
          write(98,'(a1,i0)',advance='no') char(9), arr(i)
      end do
      write(98,'(a)') ''
      return
      end


      subroutine dbgrs(phase, ctx, label, val)
c
c     Emit a single real*4 scalar.
c
      implicit none
      character*(*) phase, ctx, label
      real*4        val
c
      real*4        arr1(1)
c
      arr1(1) = val
      call dbgr1(phase, ctx, label, 1, arr1)
      return
      end


      subroutine dbgis(phase, ctx, label, val)
c
c     Emit a single integer*4 scalar.
c
      implicit none
      character*(*) phase, ctx, label
      integer*4     val
c
      integer*4     arr1(1)
c
      arr1(1) = val
      call dbgi1(phase, ctx, label, 1, arr1)
      return
      end
