%global pgmajorversion 96
%global pginstdir /usr/pgsql-9.6
%global sname plpgsql_check

Name:		%{sname}_%{pgmajorversion}
Version:	1.10.0
Release:	1%{?dist}
Summary:	Additional tools for plpgsql functions validation

Group:		Applications/Databases
License:	BSD
URL:		https://github.com/okbob/plpgsql_check/archive/v%{version}.zip
Source0:	plpgsql_check-%{version}.zip
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:	postgresql%{pgmajorversion}-devel
Requires:	postgresql%{pgmajorversion}

%description
The plpgsql_check is PostgreSQL extension with functionality for direct
or indirect extra validation of functions in plpgsql language. It verifies
a validity of SQL identifiers used in plpgsql code. It try to identify
a performance issues.

%prep
%setup -q -n %{sname}-%{version}


%build
PATH="%{pginstdir}/bin;$PATH" ; export PATH
CFLAGS="${CFLAGS:-%optflags}" ; export CFLAGS

USE_PGXS=1 make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make USE_PGXS=1 DESTDIR=%{buildroot} install

%clean
rm -rf %{buildroot}

%files
%defattr(644,root,root,755)
%doc README.md
%{pginstdir}/lib/plpgsql_check.so
%{pginstdir}/share/extension/plpgsql_check--1.7.sql
%{pginstdir}/share/extension/plpgsql_check.control

%changelog
* Fri Jun 05 2020 - Pavel STEHULE <pavel.stehule@gmail.com> 1.10.0
- deduction record type struction from result of polymorphic function

* Mon Apr 27 2020 - Pavel STEHULE <pavel.stehule@gmail.com> 1.9.1
- minor bugfixes

* Mon Mar 30 2020 - Pavel STEHULE <pavel.stehule@gmail.com> 1.9.0
- statement and branch coverage metrics
- remove support for Postgres 9.4

* Mon Jan 06 2020 - Pavel STEHULE <pavel.stehule@gmail.com> 1.8.2
- fix of compilation issue

* Sun Jan 05 2020 - Pavel STEHULE <pavel.stehule@gmail.com> 1.8.1
- cleaner detection function oid from name or signature

* Sun Dec 29 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.8.0
- use Postgres tool for calling functions from plpgsql library instead dynamic linking
- it solve issues related to dependency plpgsq_check on plpgsql

* Mon Sep 23 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.7.6
- fix false alarm - multiple plans in EXECUTE statement, and possible crash

* Tue Sep 10 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.7.5
- allow some work on tables with rules

* Wed Jul 24 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.7.3
- profiler bugfixes

* Tue May 21 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.7.2
- profiler bugfixes

* Fri Apr 26 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.7.1
- bugfixes

* Wed Apr 17 2019 - Pavel STEHULE <pavel.stehule@gmail.com> 1.7.0
- check of format of fmt string of "format" function
- better check of dynamic SQL when it is const string
- check of SQL injection vulnerability of stmt expression at EXECUTE stmt

* Fri Dec 23 2018 - Pavel STEHULE <pavel.stehule@gmail.com> 1.4.2-1
- metada fix

* Fri Dec 21 2018 - Pavel STEHULE <pavel.stehule@gmail.com> 1.4.1-1
- minor bugfixes

* Sun Dec 2 2018 - Pavel STEHULE <pavel.stehule@gmail.com> 1.4.0-1
- possible to show function's dependency on functions and tables
- integrated profiler
- bug fixes (almost false alarms)

* Wed Jun 6 2018 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.3-1
- PostgreSQL 11 support
- detect hidden casts in expressions

* Thu Oct 26 2017 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.2-1
- never read variables detection
- fix false alarm on MOVE command

* Fri Sep 15 2017 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.1-1
- missing RETURN detection
- fix some bugs and false alarms
- PostgreSQL 11 support

* Fri Now 11 2016 - Pavel STEHULE <pavel.stehule@gmail.com> 1.2.0-1
- support extra warnings - shadowed variables

* Thu Aug 25 2016 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.5-1
- minor fixes, support for PostgreSQL 10

* Fri Apr 15 2016 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.4-1
- support for PostgreSQL 9.6

* Mon Oct 12 2015 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.3-1
- fix false alarms of unused cursor variables
- fix regress tests

* Thu Jul 09 2015 - Pavel STEHULE <pavel.stehule@gmail.com> 1.0.2-2
- bugfix release

* Fri Dec 19 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.3-1
- fix a broken record field type checking
- add check for assign to array field

* Mon Aug 25 2014 - Pavel STEHULE <pavel.stehule@gmail.com> 0.9.1-1
- Initial packaging
